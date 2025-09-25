use crate::utils::constraints::*;
use crate::components::*;

use gloo_net::http::Request;
use leptos::{logging::log, prelude::*, task::spawn_local};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize)]
struct ApiRequest {
    query: String,
}

#[derive(Debug, Deserialize)]
struct ApiSelectResponse {
    pub result: Value,
}

#[component]
pub fn Catalog() -> impl IntoView {
    let (query, set_query) = signal("select * from object_store_catalog limit 1000".to_string());
    let (mode, set_mode) = signal(Mode::Select); // mode
    let (is_loading, set_is_loading) = signal(false); // spinner
    let (result, set_result) = signal(None::<Value>); // select result tabular format from select opeation
    let (error, set_error) = signal(None::<String>); // error msg

    let send_request = move |_| {
        spawn_local(async move {
            let payload = ApiRequest {
                query: query.get_untracked(),
            };
            let endpoint = format!("{URL}catalog");
            log!(
                "Sending payload: {:?} to: {:?}",
                payload,
                endpoint,
            );
            set_is_loading.set(true);
            set_error.set(None);

            let response = match Request::post(&endpoint)
                .header("Content-Type", "application/json")
                .json(&payload)
            {
                Ok(req) => match req.send().await {
                    Ok(req) => req,
                    Err(e) => {
                        set_result.set(None);
                        set_error.set(Some(format!("Network error: {e}")));
                        set_is_loading.set(false);
                        return;
                    }
                },
                Err(e) => {
                    set_result.set(None);
                    set_error.set(Some(format!("Failed to build request: {e}")));
                    set_is_loading.set(false);
                    return;
                }
            };

            if !response.ok() {
                let msg = match response.status() {
                    400 => "Invalid user input".to_string(),
                    404 => "No data found".to_string(),
                    500 => "Internal server error".to_string(),
                    _ => format!("Error {} occurred", response.status()),
                };
                set_result.set(None);
                set_error.set(Some(msg));
                set_is_loading.set(false);
                return;
            }

            match response.json::<ApiSelectResponse>().await {
                Ok(resp) => set_result.set(Some(resp.result)),
                Err(e) => {
                    set_result.set(None);
                    set_error.set(Some(format!("Failed to parse response: {e}")));
                }
            }
            set_is_loading.set(false);
        });
    };

    view! {
        // spinner
        <Spinner visible=is_loading />
        
        <div style="display: flex; flex-direction: column; align-items: center; gap: 1rem;">
            <h1>"Object Store Catalog"</h1>

            // input query form
            <QueryEditor query=query set_query=set_query />

            // error msg
            <ErrorMessage error=error />

            // operation type
            <OperationPanel
                mode=mode
                set_mode=set_mode
                send_request=send_request
                is_loading=is_loading
                modes=vec![Mode::Select]
            />
            
            // select result
            <Show when=move || mode.get() == Mode::Select>
                <CatalogResult result=result />
            </Show>
        </div>
    }
}
