use actix_web::{rt, web::{self, route}, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_ws::Message;
use log::warn;
use opentelemetry::sdk::trace;

use crate::{logprocessor, telemetry::Telemetry, Configuration};
use futures::StreamExt;

async fn receive_log(
    tracer: web::Data<trace::Tracer>,
    metric_obj: web::Data<Telemetry>,
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    let (res, mut session, mut stream) = actix_ws::handle(&req, stream)?;

    rt::spawn(async move {
        while let Some(msg) = stream.next().await {
            if let Ok(Message::Text(text)) = msg {
                let _ = logprocessor::log_processor(&text.to_string().trim(), &metric_obj, &tracer).await;
                session.text("{\"success\": true}").await.unwrap();
            }
            // TODO: log if failed
        }
    });

    Ok(res)
}

pub async fn ws_server(
    cfg: &Configuration,
    tracer: web::Data<trace::Tracer>,
    metric_obj: web::Data<Telemetry>,
) -> std::io::Result<()> {
    warn!("Starting websocket server @ {}", cfg.ws_listen_addr);
    HttpServer::new(move || {
        App::new()
            .app_data(tracer.clone())
            .app_data(metric_obj.clone())
            .route("/ws", route().to(receive_log))
    })
    .bind(&cfg.ws_listen_addr)?
    .run()
    .await
}
