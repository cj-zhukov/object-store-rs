use leptos::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct ObjectStoreTable {
    file_name: Option<String>,
    file_type: Option<String>,
    file_size: Option<u64>,
    file_path: Option<String>,
    file_url: Option<String>,
    dt: Option<String>,
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
                        let columns: Vec<(&str, Box<dyn Fn(&ObjectStoreTable) -> Option<String>>)> = vec![
                            ("file_name", Box::new(|r| r.file_name.clone())),
                            ("file_type", Box::new(|r| r.file_type.clone())),
                            ("file_size", Box::new(|r| r.file_size.map(|v| v.to_string()))),
                            ("file_path", Box::new(|r| r.file_path.clone())),
                            ("file_url", Box::new(|r| r.file_url.clone())),
                            ("dt", Box::new(|r| r.dt.clone())),
                        ];

                        let active_columns: Vec<_> = columns
                            .iter()
                            .filter(|(_, getter)| rows.iter().any(|r| getter(r).is_some()))
                            .collect();

                        view! {
                            <table style="width: 100%; border-collapse: collapse;">
                                <thead>
                                    <tr>
                                        {active_columns.iter().map(|(name, _)| view! {
                                            <th style="border: 1px solid black; padding: 8px;">{name.to_string()}</th>
                                        }).collect_view()}
                                    </tr>
                                </thead>
                                <tbody>
                                    {rows.iter().map(|row| {
                                        view! {
                                            <tr>
                                                {active_columns.iter().map(|(_, getter)| {
                                                    let value = getter(row).unwrap_or_default();
                                                    view! {
                                                        <td style="border: 1px solid black; padding: 8px;">{value}</td>
                                                    }
                                                }).collect_view()}
                                            </tr>
                                        }
                                    }).collect_view()}
                                </tbody>
                            </table>
                        }
                    })
            }}
        </div>
    }
}
