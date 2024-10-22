use actix_web::{rt, web::{self, route}, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_ws::{AggregatedMessage, Message};
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
    let (res, mut session, stream) = actix_ws::handle(&req, stream)?;

    let mut stream = stream
        .aggregate_continuations()
        // aggregate continuation frames up to 1MiB
        .max_continuation_size(2_usize.pow(20));

    rt::spawn(async move {
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(AggregatedMessage::Text(text)) => {
                    for line in text.split("\n") {
                        let _ = logprocessor::log_processor(&line.trim().to_string(), &metric_obj, &tracer).await;
                    }
                    session.text("{\"success\": true}").await.unwrap();
                }

                Ok(AggregatedMessage::Ping(msg)) => {
                    // respond to PING frame with PONG frame
                    session.pong(&msg).await.unwrap();
                }

                Err(actix_ws::ProtocolError::Overflow) => {
                    warn!("overflow error: {:#?}", msg);
                    session.text("{\"success\": false}").await.unwrap();
                }

                _ => {
                    warn!("Unhandled message: {:#?}", msg);
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
