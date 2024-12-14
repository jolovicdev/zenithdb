class Emitter:
    def __init__(self):
        self._subscribers = []

    def subscribe(self, callback):
        """
        Subscribe a callback (function) that will be called whenever 'emit' is triggered.
        The callback should accept two parameters: event_name and event_data.
        """
        if callable(callback):
            self._subscribers.append(callback)
            print(f"Subscribed to emitter: {callback}")
            print(self._subscribers)

    def unsubscribe(self, callback):
        """
        Unsubscribe a previously subscribed callback.
        """
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    def has_subscribers(self) -> bool:
        """
        Check if there are any subscribers currently listening.
        """
        return len(self._subscribers) > 0

    def emit(self, event: str, data: dict):
        """
        Emit an event to all subscribers. Only emits if there's at least one subscriber.
        """
        if self.has_subscribers():
            for subscriber in self._subscribers:
                subscriber(event, data)