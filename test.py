import unittest
from datastore.DataStore import DataStore


class TestDataStore(unittest.TestCase):
    """

    """
    def test_create(self):
        self.instance = DataStore()
        # Call the craete function with the required parameters and without ttl_seconds parameter.
        create_record = self.instance.create('krishna', '{"name":"Kirsty","age":34}')

    def test_create_with_ttl(self):
        self.instance = DataStore()
        records = self.instance.create('gopal', '{"name":"Kirsty","age":34}', 12)

    def test_int_key(self):
        self.instance = DataStore()
        self.assertFalse(self.instance.create(12, '{"name":"Kirsty","age":34}'))

    def test_key_len(self):
        self.instance = DataStore()
        with self.assertRaises(Exception):
            self.assertFalse(self.instance.create('helloworldisthefirstprograminanyprogramminglanguageeverwritten',
                                                  '{"name":"Kirsty","age":34}'))

    def test_ttl_int(self):
        self.instance = DataStore()
        with self.assertRaises(Exception):
            self.assertFalse(self.instance.create('number10',
                                                  '{"name":"Kirsty","age":34}', '34'))

    def test_delete_expired_key(self):
        self.instance = DataStore()
        with self.assertRaises(Exception):
            self.assertFalse(self.instance.delete('yummy'))

    def test_read_non_key(self):
        self.instance = DataStore()
        with self.assertRaises(Exception):
            self.assertFalse(self.instance.delete('india'))


if __name__ == "__main__":
    unittest.main()
