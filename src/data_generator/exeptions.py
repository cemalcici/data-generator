from typing import Tuple

class NotVaildExtensionError(Exception):
    "Error when the file extension is not as requested"
    
    def __init__(
        self,
        vaild_types: Tuple[str],
        message='Does not have a readable file type. Vaild Types:'
    ):
        self.message = f'{message} {vaild_types}'
        super().__init__(self.message)
