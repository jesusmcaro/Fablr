from faker.providers import BaseProvider

class MyProvider(BaseProvider):
    def foo(self) -> str:
        return 'bar'