# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
from typing import Dict, Any
from urllib.parse import quote

import apache_beam as beam
import requests
import re

from uploaders import utils
from models.execution import DestinationType, Batch


class GoogleAnalyticsEnhancedEcommMeasurementProtocolUploaderDoFn(beam.DoFn):
  def __init__(self):
    super().__init__()
    self.API_URL = "https://www.google-analytics.com/batch"
    self.UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"

  def start_bundle(self):
    pass

  def extractGroupDigits(self, text,group):
    ec_regex = re.compile('\w+\_(\d+)\_[a-zA-Z]+(\d*)')
    if ec_regex:
      return ec_regex.search(text, re.IGNORECASE).group(group)


  def _format_hit(self, payload: Dict[str, Any]) -> str:
    return "&".join([key + "=" + quote(str(value)) for key, value in payload.items() if value is not None])

  @utils.safe_process(logger=logging.getLogger("megalista.GoogleAnalyticsEnhancedEcommMeasurementProtocolUploader"))
  def process(self, batch: Batch, **kwargs):
    execution = batch.execution
    rows = batch.elements
    payloads = [{
        "v": 1,
        "t": 'event',
        "ds": "mp - megalista",
        "ni": 1,
        **{'cid': row[key] for key in row.keys() if key.startswith("client_id")},
        **{'uid': row[key] for key in row.keys() if key.startswith("user_id")},
        "ti": row["transaction_id"],
        "tr": row.get("revenue"), 
        "tt": row.get("tax"), 
        "ts": row.get("shipping"),
        **{'pr'+self.extractGroupDigits(key,1)+'id': row[key] for key in row.keys() if re.match('product\_\d+\_sku', key)},
        **{'pr'+self.extractGroupDigits(key,1)+'nm': row[key] for key in row.keys() if re.match('product\_\d+\_name', key)},
        **{'pr'+self.extractGroupDigits(key,1)+'br': row[key] for key in row.keys() if re.match('product\_\d+\_brand', key)},
        **{'pr'+self.extractGroupDigits(key,1)+'ca': row[key] for key in row.keys() if re.match('product\_\d+\_category', key)},
        **{'pr'+self.extractGroupDigits(key,1)+'va': row[key] for key in row.keys() if re.match('product\_\d+\_variant', key)},
        **{'pr'+self.extractGroupDigits(key,1)+'pr': row[key] for key in row.keys() if re.match('product\_\d+\_price', key)},
        **{'pr'+self.extractGroupDigits(key,1)+'pr': row[key] for key in row.keys() if re.match('product\_\d+\_quantity', key)},
        **{'pr'+self.extractGroupDigits(key,1)+'cc': row[key] for key in row.keys() if re.match('product\_\d+\_cuponcode', key)},
        **{'pr'+self.extractGroupDigits(key,1)+'ps': row[key] for key in row.keys() if re.match('product\_\d+\_position', key)},
        **{'pr'+self.extractGroupDigits(key,1)+'cd'+self.extractGroupDigits(key,2): row[key] for key in row.keys() if re.match('product\_\d+\_cd\d+',key)},
        **{'pr'+self.extractGroupDigits(key,1)+'cm'+self.extractGroupDigits(key,2): row[key] for key in row.keys() if re.match('product\_\d+\_cm\d+',key)},
        "pa": row['product_action'],
        "pal": row.get('product_action_list'),
        "cos": row.get('checkout_step'),
        "col": row.get('checkout_step_option'),
        "ea": row['event_action'],
        "ec": row['event_category'],
        "ev": row.get('event_value'),
        "el": row.get('event_label'),
        "ua": self.UA,
        **{key: row[key] for key in row.keys() if re.match('c[dm]\d+',key)}
    } for row in rows]

    encoded = [self._format_hit(payload) for payload in payloads]

    payload = '\n'.join(encoded)
    response = requests.post(url=self.API_URL, data=payload)
    if response.status_code != 200:
      raise Exception(
        f"Error uploading to Analytics HTTP {response.status_code}: {response.raw}")
    else:
      yield batch
