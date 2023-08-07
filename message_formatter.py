from dataclasses import dataclass
from reactivex import operators as ops, Observable


@dataclass
class Message:
	date: str
	price: float
	stock: str
	processed_timestamp: int


def map_to_msg(event):
	print(f'mapping event: {event}')
	return Message(str(event.get("Date")),
				   float(event.get("Close")),
				   str(event.get("Stock")),
				   int(event.get("Processed_time"))
				   )


def process(source_of_msg: Observable[dict[str, str]]) -> Observable[Message]:
	return source_of_msg.pipe(
		ops.map(lambda x: map_to_msg(x))
	)
