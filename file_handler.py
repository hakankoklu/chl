from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from file_processor import FileProcessor
import time
import logging

logging.basicConfig(filename="sample.log", level=logging.DEBUG)

class FileHandler(PatternMatchingEventHandler):

    def __init__(self):
        PatternMatchingEventHandler.__init__(self, patterns=["*.txt"])
        self.file_processor = FileProcessor()

    def on_created(self, event):
        logging.info("New file: {file}".format(file=event.src_path))
        self.process(event)

    def process(self, event):
        filename = event.src_path
        data = self.file_processor.parse_data(filename)

event_handler = FileHandler()
observer = Observer()
observer.schedule(event_handler, path='./data', recursive=False)
observer.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
