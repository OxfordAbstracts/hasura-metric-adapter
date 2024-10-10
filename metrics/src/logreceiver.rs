use actix_web::{rt, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_ws::AggregatedMessage;
use futures_util::StreamExt as _;
use log::warn;

use crate::{Configuration, logprocessor, Telemetry};
use opentelemetry::sdk::trace;

async fn receive_log(tracer: &trace::Tracer, metric_obj: &Telemetry, req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    let (res, mut session, stream) = actix_ws::handle(&req, stream)?;

    let mut stream = stream
        .aggregate_continuations()
        // aggregate continuation frames up to 1MiB
        .max_continuation_size(2_usize.pow(20));

    // start task but don't wait for it
    rt::spawn(async move {
        // receive messages from websocket
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(AggregatedMessage::Text(text)) => {
                    logprocessor::log_processor(&text, metric_obj, tracer);
                }

                // Ok(AggregatedMessage::Binary(bin)) => {
                //     // echo binary message
                //     session.binary(bin).await.unwrap();
                // }

                Ok(AggregatedMessage::Ping(msg)) => {
                    // respond to PING frame with PONG frame
                    session.pong(&msg).await.unwrap();
                }

                _ => {}
            }
        }
    });

    // respond immediately with response connected to WS session
    Ok(res)
}

// #[actix_web::main]
pub async fn ws_server(cfg: &Configuration, tracer: &trace::Tracer, metric_obj: &Telemetry) -> std::io::Result<()> {
    warn!("Starting websocket server @ {}", cfg.ws_listen_addr);
    HttpServer::new(|| App::new().route("/", web::get().to(receive_log)))
        .bind(&cfg.ws_listen_addr)?
        .run()
        .await
}
