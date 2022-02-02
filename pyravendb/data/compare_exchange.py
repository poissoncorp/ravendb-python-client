from typing import Optional


class StartingWithOptions:
    def __init__(self, start_with: str, start: Optional[int] = None, page_size: Optional[int] = None):
        self.starts_with = start_with
        self.start = start
        self.page_size = page_size
