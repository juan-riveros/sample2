from typing import Callable, NewType, cast
import bisect

MessageName = NewType("MessageName", str)
Priority = NewType("Priority", int)

class MessageBus:
    def __init__(self,):
        self.handlers: dict[MessageName, list[tuple[Priority, Callable]]] = {}

    def add_message_handler(
        self, msg_name: MessageName, handler: Callable, priority:Priority=Priority(128)
    ):
        handlers = self.handlers.setdefault(msg_name, [])
        if (priority, handler) in handlers:
            return
        bisect.insort(handlers, (priority, handler), key=lambda x: x[0])

    def del_message_handler(
        self, msg_name:MessageName, handler:Callable
    ):
        handlers = self.handlers.setdefault(msg_name, [])
        for _p, _h in handlers:
            if handler == _h:
                handlers.remove((_p, _h))
    
    def clear_message_handlers(self, msg_name: MessageName):
        del self.handlers[msg_name]

    def fire(self, msg_name: MessageName, *args, **kwargs):
        handlers = self.handlers.get(msg_name, [])
        for _p, handler in handlers:
            handler(*args, **kwargs)

    def register(self, msg_name: MessageName|str):

        def decorator(func:Callable) -> Callable:
            self.add_message_handler(cast(MessageName,msg_name), func)
            return func
        return decorator