use reqwest::Request;
use reqwest::Response;
use reqwest_middleware::{Middleware, Next, Result as MiddlewareResult};

use std::time::{SystemTime, UNIX_EPOCH};

use task_local_extensions::Extensions;
use uuid::Uuid;

pub struct UuidMiddleware;

#[async_trait::async_trait]
impl Middleware for UuidMiddleware {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> MiddlewareResult<Response> {
        let request_id = Uuid::new_v4();
        let request_guid = Uuid::new_v4();
        let client_start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut new_req = req.try_clone().unwrap();

        // Modify the request URL to include the new UUIDs and client start time
        let url = new_req.url_mut();

        let query = format!(
            "{}&clientStartTime={client_start_time}&requestId={request_id}&request_guid={request_guid}",
            url.query().unwrap_or("")
        );

        url.set_query(Some(query.as_str()));
        next.run(new_req, extensions).await
    }
}
