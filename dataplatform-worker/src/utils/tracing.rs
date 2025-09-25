use color_eyre::eyre::Result;
use tracing::Level;

pub fn init_tracing() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(Level::INFO)
        .with_current_span(true)
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .init();
    Ok(())
}
