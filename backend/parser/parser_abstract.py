from abc import ABC, abstractmethod

class ParserInterface(ABC):
    @abstractmethod
    def stop(self) -> None:
        ...

    @abstractmethod
    def run(self) -> None:
        ...
