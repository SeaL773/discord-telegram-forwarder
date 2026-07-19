from typing import Any

class _Mark:
    asyncio: Any
    parametrize: Any

mark: _Mark

def raises(expected_exception: type[BaseException]) -> Any: ...
