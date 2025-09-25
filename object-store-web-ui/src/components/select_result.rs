use leptos::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct ObjectStoreTable {
    file_name: String,
    file_type: String,
    file_size: u64,
    file_path: String,
    file_url: String,
    dt: String,
}

#[component]
pub fn SelectResult(result: ReadSignal<Option<serde_json::Value>>) -> impl IntoView {
    view! {
        <div style="width: 100%; text-align: center;">
            {move || {
                result
                    .get()
                    .and_then(|data| serde_json::from_value::<Vec<ObjectStoreTable>>(data).ok())
                    .map(|rows| {
                        view! {
                            <table style="width: 100%; border-collapse: collapse;">
                                <thead>
                                    <tr>
                                        <th style="border: 1px solid black; padding: 8px;">"file_name"</th>
                                        <th style="border: 1px solid black; padding: 8px;">"file_type"</th>
                                        <th style="border: 1px solid black; padding: 8px;">"file_size"</th>
                                        <th style="border: 1px solid black; padding: 8px;">"file_path"</th>
                                        <th style="border: 1px solid black; padding: 8px;">"file_url"</th>
                                        <th style="border: 1px solid black; padding: 8px;">"dt"</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {rows.iter().map(|row| view! {
                                        <tr>
                                            <td style="border: 1px solid black; padding: 8px;">{row.file_name.clone()}</td>
                                            <td style="border: 1px solid black; padding: 8px;">{row.file_type.clone()}</td>
                                            <td style="border: 1px solid black; padding: 8px;">{row.file_size}</td>
                                            <td style="border: 1px solid black; padding: 8px;">{row.file_path.clone()}</td>
                                            <td style="border: 1px solid black; padding: 8px;">{row.file_url.clone()}</td>
                                            <td style="border: 1px solid black; padding: 8px;">{row.dt.clone()}</td>
                                        </tr>
                                    }).collect_view()}
                                </tbody>
                            </table>
                        }
                    })
                }
            }
        </div>
    }
}
