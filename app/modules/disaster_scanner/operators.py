from typing import Callable, TypeVar

from aiostream import pipable_operator, stream, streamcontext

T = TypeVar("T")
TKey = TypeVar("TKey")


@pipable_operator
async def deduplicator(source, key_extractor: Callable[[T], TKey]):
    seen_keys = set()

    async with streamcontext(source) as streamer:
        async for item in streamer:
            key = key_extractor(item)
            if key not in seen_keys:
                seen_keys.add(key)

                if len(seen_keys) > 10000:
                    seen_keys.clear()

                yield item
