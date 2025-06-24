use clap::{Args, Parser};
use malachitebft_app::node::Node;
use reth::builder::NodeHandle;
use reth_malachite::app::node::RethNode;
use reth_malachite::app::{Config, Genesis, State, ValidatorInfo};
use reth_malachite::cli::{Cli, MalachiteChainSpecParser};
use reth_malachite::consensus::config::EngineConfig;
use reth_malachite::consensus::node::MalachiteNode;
use reth_malachite::context::MalachiteContext;
use reth_malachite::types::Address;
use std::path::PathBuf;

/// No Additional arguments
#[derive(Debug, Clone, Copy, Default, Args)]
#[non_exhaustive]
struct NoArgs;

fn main() -> eyre::Result<()> {
    reth_cli_util::sigsegv_handler::install();

    // Initialize the runtime for async operations
    let runtime = tokio::runtime::Runtime::new()?;

    runtime.block_on(async {
        // Create the context and initial state
        let ctx = MalachiteContext::default();
        let config = Config::new();

        // Create a genesis with initial validators
        let validator_address = Address::new([1; 20]);
        let validator_info = ValidatorInfo::new(validator_address, 1000, vec![0; 32]);
        let genesis = Genesis::new("1".to_string()).with_validators(vec![validator_info]);

        // Create the node address (in production, derive from public key)
        let address = Address::new([0; 20]);

        Cli::<MalachiteChainSpecParser, NoArgs>::parse().run(|builder, _: NoArgs| async move {
            // Create the application state
            let state = State::new(ctx.clone(), config, genesis.clone(), address);

            // Launch the Reth node
            let reth_node = RethNode::new(state.clone());
            let NodeHandle {
                node: _,
                node_exit_future,
            } = builder.node(reth_node).launch().await?;

            // Get the home directory
            let home_dir = PathBuf::from("./data"); // In production, use proper data dir

            // Create Malachite consensus engine configuration
            let engine_config = EngineConfig::new(
                "reth-malachite-1".to_string(),
                "node-0".to_string(),
                "127.0.0.1:26657".parse()?,
            );

            // Create the Malachite consensus node
            let malachite_node = MalachiteNode::new(engine_config, home_dir, state);

            // Start the consensus engine using the run method
            let consensus_future = tokio::spawn(async move {
                if let Err(e) = malachite_node.run().await {
                    tracing::error!("Consensus engine error: {}", e);
                }
            });

            tracing::info!("Malachite consensus engine started");

            // Wait for the node to exit
            tokio::select! {
                _ = node_exit_future => {
                    tracing::info!("Reth node exited");
                }
                _ = consensus_future => {
                    tracing::info!("Consensus engine exited");
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Received shutdown signal");
                }
            }

            Ok(())
        })
    })
}
