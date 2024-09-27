import bytewax
from bytewax.connectors.kafka import KafkaSource
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink

flow_id = "mi_flujo"
flow = Dataflow(flow_id)

kafka_source = KafkaSource(
    brokers=["161.132.40.126:29092"],
    topics=["hola"]
)

def process_message(message):
    print(f"Mensaje recibido: {message}")
    return message

inp = op.input("input", flow, kafka_source)
processed = op.map("process", inp, process_message)

op.inspect("inspeccion", processed)

op.output("salida", processed, StdOutSink())

if __name__ == "__main__":
    import bytewax.run
    bytewax.run(flow)
