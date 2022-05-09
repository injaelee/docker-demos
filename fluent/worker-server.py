from aiohttp import web
import asyncio

@asyncio.coroutine
def error_middleware(app, handler):

    @asyncio.coroutine
    def middleware_handler(request):
        try:
            response = yield from handler(request)
            return response
        except web.HTTPException as ex:
            resp = web.Response(body=str(ex), status=ex.status)
            return resp
        except Exception as ex:
            resp = web.Response(body=str(ex), status=500)
            return resp


async def ping(
    request: web.Request,
) -> web.Response:
    return web.Response(text='test')

if __name__ == '__main__':
    app = web.Application()#middlewares=[error_middleware])
    #app.router.add_get("/ping", ping)
    app.add_routes([web.get('/ping', ping)])

    web.run_app(app, port=18080)