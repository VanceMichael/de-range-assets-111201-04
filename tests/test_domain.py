import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        source = AssetGateway(self.payload['files'])
        status, headers, raw = source.fetch('map.bin', start=2, validator='"a1"')
        self.assertEqual((status, raw), (206, b'cdef'))
        self.assertEqual(source.fetch('map.bin', start=6)[0], 416)
        with self.assertRaises(ValueError):
            verify(b'bad', 6, 'x')

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))
