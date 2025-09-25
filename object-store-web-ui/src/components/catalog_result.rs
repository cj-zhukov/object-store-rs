use leptos::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct ObjectStoreCatalogTable {
    pub year: Option<String>,
    pub file_type: Option<String>,
    pub cnt_file_type: Option<i64>,
    pub sum_file_size: Option<i64>,
}

#[component]
pub fn CatalogResult(result: ReadSignal<Option<serde_json::Value>>) -> impl IntoView {
    view! {
        <div style="width: 100%; text-align: center;">
            {move || {
                result
                    .get()
                    .and_then(|data| serde_json::from_value::<Vec<ObjectStoreCatalogTable>>(data).ok())
                    .map(|rows| {
                        view! {
                            <table style="width: 100%; border-collapse: collapse;">
                                <thead>
                                    <tr>
                                        <th style="border: 1px solid black; padding: 8px;">"year"</th>
                                        <th style="border: 1px solid black; padding: 8px;">"file_type"</th>
                                        <th style="border: 1px solid black; padding: 8px;">"cnt_file_type"</th>
                                        <th style="border: 1px solid black; padding: 8px;">"sum_file_size"</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {rows.iter().map(|row| view! {
                                        <tr>
                                            <td style="border: 1px solid black; padding: 8px;">{row.year.clone()}</td>
                                            <td style="border: 1px solid black; padding: 8px;">{row.file_type.clone()}</td>
                                            <td style="border: 1px solid black; padding: 8px;">{row.cnt_file_type}</td>
                                            <td style="border: 1px solid black; padding: 8px;">{row.sum_file_size}</td>
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
