import time
import asyncio
import reactivex
from requests_html import AsyncHTMLSession
from reactivex import Observable
from reactivex.abc import ObserverBase
from reactivex.disposable import Disposable
from typing import Union

async def get_data(stock, s, url, headers):
	try:
		res = await s.get(url+stock, headers=headers)
		decoded = res.content.decode('utf-8').split('\n')
		data = dict(zip(decoded[0].split(','), decoded[1].split(',')))
		data.update({"Stock": stock})
		data.update({"Processed_time": round(time.time() * 1000)})
		return data
	except Exception as e:
		print(e)


async def download_stock(observer, stock):
	url = f'https://query1.finance.yahoo.com/v7/finance/download/'
	headers = {
		'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
	}
	s = AsyncHTMLSession()
	while True:
		result = await get_data(stock, s, url, headers)

		await asyncio.sleep(10)
		observer.on_next(result)


def connect(subscription_loop: asyncio.AbstractEventLoop, stock: str) -> Observable[dict[str, Union[str, float]]]:
	def on_subscribe(observer: ObserverBase, scheduler):
		task = asyncio.run_coroutine_threadsafe(download_stock(observer, stock=stock), loop=subscription_loop)

		return Disposable(lambda: task.cancel())
	return reactivex.create(on_subscribe)

