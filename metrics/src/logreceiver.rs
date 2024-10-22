use actix_web::{rt, web::{self, route}, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_ws::{AggregatedMessage, Message};
use log::{warn, error};
use opentelemetry::sdk::trace;

use crate::{logprocessor, telemetry::Telemetry, Configuration};
use futures::StreamExt;

async fn receive_log(
    tracer: web::Data<trace::Tracer>,
    metric_obj: web::Data<Telemetry>,
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    let (res, mut session, stream) = actix_ws::handle(&req, stream)?;

    let two_megs = 2 * 1024 * 1024;

    let mut stream = stream
        .max_frame_size(two_megs)
        .aggregate_continuations()
        // aggregate continuation frames up to 1MiB
        .max_continuation_size(two_megs);

    rt::spawn(async move {
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(AggregatedMessage::Text(text)) => {
                    for line in text.trim().split("\n") {
                        let _ = logprocessor::log_processor(&line.trim().to_string(), &metric_obj, &tracer).await;
                    }
                    session.text("{\"success\": true}").await.unwrap();
                }

                Ok(AggregatedMessage::Ping(msg)) => {
                    // respond to PING frame with PONG frame
                    session.pong(&msg).await.unwrap();
                }

                Err(e) => {
                    error!("Error parsing message: {:#?}", e);
                    session.text("{\"success\": false}").await.unwrap();
                }

                e => {
                    warn!("Unhandled message type: {:#?}", e);
                    session.text("{\"success\": false}").await.unwrap();
                }
            }
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
