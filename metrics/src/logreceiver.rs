use actix_web::{web::{self, route}, App, Error, HttpRequest, HttpResponse, HttpServer};
use log::warn;
use opentelemetry::sdk::trace;

use crate::{logprocessor, telemetry::Telemetry, Configuration};

// async fn receive_log(tracer: &trace::Tracer, metric_obj: &Telemetry, req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
//     let (res, mut session, stream) = actix_ws::handle(&req, stream)?;

//     let mut stream = stream
//         .aggregate_continuations()
//         // aggregate continuation frames up to 1MiB
//         .max_continuation_size(2_usize.pow(20));

//     // start task but don't wait for it
//     rt::spawn(async move {
//         // receive messages from websocket
//         while let Some(msg) = stream.next().await {
//             match msg {
//                 Ok(AggregatedMessage::Text(text)) => {
//                     logprocessor::log_processor(&text, metric_obj, tracer);
//                 }

//                 // Ok(AggregatedMessage::Binary(bin)) => {
//                 //     // echo binary message
//                 //     session.binary(bin).await.unwrap();
//                 // }

//                 Ok(AggregatedMessage::Ping(msg)) => {
//                     // respond to PING frame with PONG frame
//                     session.pong(&msg).await.unwrap();
//                 }

//                 _ => {}
//             }
//         }
//     });

//     // respond immediately with response connected to WS session
//     Ok(res)
// }

async fn receive_log(
    tracer: web::Data<trace::Tracer>,
    metric_obj: web::Data<Telemetry>,
    _req: HttpRequest,
    _stream: web::Payload,
) -> Result<HttpResponse, Error> {
    let text = "test".to_string();
    let _ = logprocessor::log_processor(&text, &metric_obj, &tracer).await;
    return Ok(HttpResponse::Ok().finish());
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
