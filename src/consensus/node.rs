//! Node trait implementation for Malachite consensus engine

use crate::{
    app::{Genesis, State},
    consensus::config::{EngineConfig, NodeConfig},
    context::{BasePeerAddress, MalachiteContext},
    provider::{Ed25519Provider, PrivateKey, PublicKey},
    types::Address,
};
use async_trait::async_trait;
use malachitebft_app::events::{RxEvent, TxEvent};
use malachitebft_app::node::{
    CanGeneratePrivateKey, CanMakeConfig, CanMakeGenesis, CanMakePrivateKeyFile, EngineHandle,
    MakeConfigSettings, Node, NodeHandle,
};
use malachitebft_app::types::core::VotingPower;
use malachitebft_app::types::Keypair;
use rand::{CryptoRng, RngCore};
use std::path::PathBuf;
use tokio::task::JoinHandle;

/// Implementation of Malachite's Node trait for reth-malachite
#[derive(Clone)]
pub struct MalachiteNode {
    /// Engine configuration
    pub config: EngineConfig,
    /// Path to the home directory
    pub home_dir: PathBuf,
    /// Path to the genesis file
    pub genesis_file: PathBuf,
    /// Path to the private key file
    pub private_key_file: PathBuf,
    /// Application state
    pub app_state: State,
}

impl MalachiteNode {
    /// Create a new node implementation
    pub fn new(config: EngineConfig, home_dir: PathBuf, app_state: State) -> Self {
        let genesis_file = home_dir.join("genesis.json");
        let private_key_file = home_dir.join("priv_validator_key.json");

        Self {
            config,
            home_dir,
            genesis_file,
            private_key_file,
            app_state,
        }
    }
}

/// Handle for the running consensus node
pub struct ConsensusHandle {
    /// Application task handle
    pub app: JoinHandle<()>,
    /// Engine handle from Malachite
    pub engine: EngineHandle,
    /// Event transmitter
    pub tx_event: TxEvent<MalachiteContext>,
}

#[async_trait]
impl NodeHandle<MalachiteContext> for ConsensusHandle {
    fn subscribe(&self) -> RxEvent<MalachiteContext> {
        self.tx_event.subscribe()
    }

    async fn kill(&self, _reason: Option<String>) -> eyre::Result<()> {
        self.engine.actor.kill_and_wait(None).await?;
        self.app.abort();
        self.engine.handle.abort();
        Ok(())
    }
}

#[async_trait]
impl Node for MalachiteNode {
    type Context = MalachiteContext;
    type Config = NodeConfig;
    type Genesis = Genesis;
    type PrivateKeyFile = PrivateKey;
    type SigningProvider = Ed25519Provider;
    type NodeHandle = ConsensusHandle;

    fn get_home_dir(&self) -> PathBuf {
        self.home_dir.clone()
    }

    fn load_config(&self) -> eyre::Result<Self::Config> {
        Ok(self.config.node.clone())
    }

    fn get_address(&self, pk: &PublicKey) -> BasePeerAddress {
        // Convert public key to address
        // For now, use a simple derivation - in production this would follow the chain's address scheme
        let pk_bytes = pk.as_bytes();
        let mut addr_bytes = [0u8; 20];
        addr_bytes.copy_from_slice(&pk_bytes[..20]);
        BasePeerAddress::from(Address::new(addr_bytes))
    }

    fn get_public_key(&self, pk: &PrivateKey) -> PublicKey {
        pk.public_key()
    }

    fn get_keypair(&self, pk: PrivateKey) -> Keypair {
        // Convert our private key to Malachite's Keypair type
        let sk_bytes = pk.inner().to_bytes();
        Keypair::ed25519_from_bytes(sk_bytes).expect("valid ed25519 key")
    }

    fn load_private_key(&self, file: Self::PrivateKeyFile) -> PrivateKey {
        file
    }

    fn load_private_key_file(&self) -> eyre::Result<Self::PrivateKeyFile> {
        // For now, generate a new key if file doesn't exist
        // In production, this would load from the file or error if not found
        if self.private_key_file.exists() {
            let contents = std::fs::read_to_string(&self.private_key_file)?;
            let key: PrivateKey = serde_json::from_str(&contents)?;
            Ok(key)
        } else {
            // Generate a new key for testing
            let key = PrivateKey::generate(rand::thread_rng());
            Ok(key)
        }
    }

    fn get_signing_provider(
        &self,
        _private_key: malachitebft_core_types::PrivateKey<MalachiteContext>,
    ) -> Self::SigningProvider {
        // Convert malachitebft_core_types::PrivateKey to our LocalPrivateKey
        // For now, this is a placeholder
        Ed25519Provider::new_test()
    }

    fn load_genesis(&self) -> eyre::Result<Self::Genesis> {
        Ok(self.app_state.genesis.clone())
    }

    async fn start(&self) -> eyre::Result<ConsensusHandle> {
        let config = self.load_config()?;
        let private_key_file = self.load_private_key_file()?;
        let private_key = self.load_private_key(private_key_file);
        let public_key = self.get_public_key(&private_key);
        let _address = self.get_address(&public_key);
        let _signing_provider = self.get_signing_provider(private_key);
        let ctx = self.app_state.ctx.clone();

        let _genesis = self.load_genesis()?;
        let initial_validator_set = self.app_state.get_validator_set(crate::height::Height(1));

        // Start the Malachite consensus engine
        let (mut channels, engine_handle) = malachitebft_app_channel::start_engine(
            ctx.clone(),
            self.clone(),
            config,                   // Convert to Malachite's config type
            crate::codec::ProtoCodec, // WAL codec
            crate::codec::ProtoCodec, // Network codec
            self.config.start_height,
            initial_validator_set,
        )
        .await?;

        let tx_event = channels.events.clone();

        // Spawn the application handler task
        let mut app_state = self.app_state.clone();
        let app_handle = tokio::spawn(async move {
            if let Err(e) =
                super::handler::run_consensus_handler(&mut app_state, &mut channels).await
            {
                tracing::error!(%e, "Consensus handler error");
            }
        });

        Ok(ConsensusHandle {
            app: app_handle,
            engine: engine_handle,
            tx_event,
        })
    }

    async fn run(self) -> eyre::Result<()> {
        let handle = self.start().await?;
        handle.app.await.map_err(Into::into)
    }
}

impl CanMakeGenesis for MalachiteNode {
    fn make_genesis(&self, validators: Vec<(PublicKey, VotingPower)>) -> Self::Genesis {
        // Create genesis with the given validators
        let validator_infos = validators
            .into_iter()
            .map(|(_pk, vp)| {
                // For now, use a placeholder conversion
                // In production, derive address properly from public key
                let address = Address::new([0u8; 20]);
                let pk_bytes = vec![0u8; 32]; // Placeholder

                crate::app::ValidatorInfo::new(address, vp, pk_bytes)
            })
            .collect();

        Genesis::new(self.config.network.chain_id.clone()).with_validators(validator_infos)
    }
}

impl CanGeneratePrivateKey for MalachiteNode {
    fn generate_private_key<R>(&self, rng: R) -> PrivateKey
    where
        R: RngCore + CryptoRng,
    {
        PrivateKey::generate(rng)
    }
}

impl CanMakePrivateKeyFile for MalachiteNode {
    fn make_private_key_file(&self, private_key: PrivateKey) -> Self::PrivateKeyFile {
        private_key
    }
}

impl CanMakeConfig for MalachiteNode {
    fn make_config(index: usize, _total: usize, _settings: MakeConfigSettings) -> Self::Config {
        // For now, return a default config
        // In production, this would generate appropriate config for node index out of total
        NodeConfig::new(
            format!("node-{index}"),
            format!("127.0.0.1:{}", 26000 + index),
            Vec::new(),
        )
    }
}
