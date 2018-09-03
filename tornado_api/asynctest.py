import unittest
import asyncio


# close the default event loop
asyncio.get_event_loop().close()


class AsyncTestCase(unittest.TestCase):

    def setUpEventLoop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDownEventLoop(self):
        self.loop.close()

    async def asyncSetUp(self):
        pass

    async def asyncTearDown(self):
        pass

    def asyncWrapper(self, func):
        def wrapper():
            self.setUpEventLoop()
            self.loop.run_until_complete(self.asyncSetUp())
            self.loop.run_until_complete(func())
            self.loop.run_until_complete(self.asyncTearDown())
            self.tearDownEventLoop()
        return wrapper

    def __getattribute__(self, name):
        attr = super(AsyncTestCase, self).__getattribute__(name)
        if name.startswith('test_') and asyncio.iscoroutinefunction(attr):
            return self.asyncWrapper(attr)
        return attr
