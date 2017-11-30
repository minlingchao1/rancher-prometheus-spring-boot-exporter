#!/usr/bin/env python

import requests
import json

import threading
import argparse
import re

from prometheus_client import generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST, Counter, Gauge, Summary
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlparse


class _ThreadingSimpleServer(ThreadingMixIn, HTTPServer):
    """Thread per request HTTP server."""

class MetricsHandler(BaseHTTPRequestHandler):
  """HTTP handler that gives metrics from ``core.REGISTRY``."""

  def do_GET(self):
    registry = generate_registry()
    params = parse_qs(urlparse(self.path).query)
    if 'name[]' in params:
      registry = registry.restricted_registry(params['name[]'])
    try:
      output = generate_latest(registry)
    except:
      self.send_error(500, 'error generating metric output')
      raise
    self.send_response(200)
    self.send_header('Content-Type', CONTENT_TYPE_LATEST)
    self.end_headers()
    self.wfile.write(output)

  def log_message(self, format, *args):
    """Log nothing."""
        
def start_http_server(port, addr=''):
  """Starts an HTTP server for prometheus metrics as a daemon thread"""
  httpd = _ThreadingSimpleServer((addr, port), MetricsHandler)
  t = threading.Thread(target=httpd.serve_forever)
  t.start()

class RancherClient:
  def __init__(self, rancherApiUrl, filter = None):
    self.rancherApiUrl = rancherApiUrl
    self.filterPattern = None if not filter else filter.split(',')
    self.load_hosts_metadata()
    self.self_host = self.get_self_hostname()
  
  def load_hosts_metadata(self):
    data = self.load_url(self.rancherApiUrl+'hosts')
    self.hosts_metadata = {}
    for host in data['data']:
      self.hosts_metadata[host['id']] = host['hostname']
  
  def load_url(self, url):
    response = requests.get(url)
    return json.loads(response.content)  
  
  def _get_spring_boot_app_host(self, container):
    return self.hosts_metadata[container['hostId']]
  
  def _extract_spring_boot_apps(self, containers, result):
    for container in containers:
      data = {'image': container['imageUuid'],
        'name': container['name'],
        'ip': container['primaryIpAddress'],
        'stack-name': container['labels'].get('io.rancher.stack.name', None),
        'state': container['state'],
        'host': self._get_spring_boot_app_host(container)}
      
      if self.filter(data):
        result.append(data)
  
  def get_spring_boot_apps(self):
    url = self.rancherApiUrl + 'containers'
    
    result = []
    while url:
      parsed_data = self.load_url(url)
      containers = parsed_data['data']
      self._extract_spring_boot_apps(containers, result)
      url = parsed_data['pagination']['next']
    return result
    
  def get_self_hostname(self):
    return requests.get('http://rancher-metadata/2015-12-19/self/host/hostName').content.decode('utf-8')
    
  def filter(self, data):
    if data['host'] != self.self_host:
      return False

    if data['state'] == 'stopped':
      return False
    
    if not self.filterPattern:
      return True
    
    for pattern in self.filterPattern:
      if data['image'].find(pattern) != -1:
        return True
    return False

class MetricsReader:
  def __init__(self, registry):
    self.registry = registry
    self._counters = {}
    self._gauges = {}
    self._summaries = {}
  
  def _format_metrics_url(self, app):
    return "http://{url}:8080/metrics".format(url=app['ip'])
    
  def load_metrics(self, app):
    url = self._format_metrics_url(app)
    try:
      response = requests.get(url, timeout=60)
    except Exception as err:
      print("Catch "+str(err))
      return None
      
    return json.loads(response.content)

  def _register_counter(self, app, metric_name, value):
    if not metric_name in self._counters:
      counter = Counter(metric_name, metric_name, ['name', 'stackName', 'ip'], registry=self.registry)
      self._counters[metric_name] = counter
      
    self._counters[metric_name].labels(name=app['name'], stackName=app['stack-name'], ip=app['ip']).inc(value)
        
  def _register_gauge(self, app, metric_name, value):
    if not metric_name in self._gauges:
      gauge = Gauge(metric_name, metric_name, ['name', 'stackName', 'ip'], registry=self.registry)
      self._gauges[metric_name] = gauge
      
    self._gauges[metric_name].labels(name=app['name'], stackName=app['stack-name'], ip=app['ip']).set(value)
  
  def _register_summary(self, app, metric_name, value):
    if not metric_name in self._summaries:
      summary = Summary(metric_name, metric_name, ['name', 'stackName', 'ip'], registry=self.registry)
      self._summaries[metric_name] = summary
    
    self._summaries[metric_name].labels(name=app['name'], stackName=app['stack-name'], ip=app['ip']).observe(value)
  
  def register_metrics(self, app):
    metrics = self.load_metrics(app)
    if metrics is None:
      return
    
    for m in metrics:
      metric_name = re.sub(r'[\.\-]', '_', m)
      
      if m.find('counter') == 0:
        self._register_counter(app, metric_name[8:], metrics[m])
      elif m.find('gauge') == 0:
        self._register_gauge(app, metric_name[6:], metrics[m])
      else:
        self._register_summary(app, metric_name, metrics[m])
        
PARSED = None        
def generate_registry():
  registry = CollectorRegistry(auto_describe=True)
  reader = MetricsReader(registry)
  client = RancherClient(PARSED.rancher, PARSED.image_filter)
  for app in client.get_spring_boot_apps():
    reader.register_metrics(app)
  return registry
        
def main():
  global PARSED
  parser = argparse.ArgumentParser()
  parser.add_argument('--rancher', help='Rancher REST URL', required=True)
  parser.add_argument('--image-filter', help='Pattern for filter containers (image based)', required=False)
  parser.add_argument('--test', help='Output data to stdout and exit', action='store_true', required=False)
  PARSED = parser.parse_args()
  
  if PARSED.test:
    registry = generate_registry()
    print(generate_latest(registry))
  else:
    print('Listen on 0.0.0.0:8080')
    start_http_server(8080)
    
if __name__ == "__main__":
  main()
