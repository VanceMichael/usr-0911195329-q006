import unittest
from app import health_payload
class T(unittest.TestCase):
 def test_health(self): self.assertEqual(health_payload()["status"],"ok")
if __name__=="__main__": unittest.main()
