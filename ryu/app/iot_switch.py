from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_0
from ryu.lib.mac import haddr_to_bin
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
from collections import defaultdict

from ryu.lib.dpid import dpid_to_str
import os

from webob.static import DirectoryApp

from ryu.app.wsgi import ControllerBase, WSGIApplication, route
from ryu.base import app_manager

import mysql.connector

import os
import time
import datetime
import random 

PATH = os.path.dirname(__file__)

class Graph:
  def __init__(self):
    self.nodes = set()
    self.edges = defaultdict(list)
    self.distances = {}

  def add_node(self, value):
    self.nodes.add(value)

  def add_edge(self, from_node, to_node, distance):
    self.edges[from_node].append(to_node)
    self.edges[to_node].append(from_node)
    self.distances[(from_node, to_node)] = distance


def dijsktra(graph, initial):
  visited = {initial: 0}
  path = {}

  nodes = set(graph.nodes)

  while nodes: 
    min_node = None
    for node in nodes:
      if node in visited:
        if min_node is None:
          min_node = node
        elif visited[node] < visited[min_node]:
          min_node = node

    if min_node is None:
      break

    nodes.remove(min_node)
    current_weight = visited[min_node]

    for edge in graph.edges[min_node]:
      weight = current_weight + graph.distances[(min_node, edge)]
      if edge not in visited or weight < visited[edge]:
        visited[edge] = weight
        path[edge] = min_node

  return visited, path



class SimpleSwitch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION]
    _CONTEXTS = {
      'wsgi': WSGIApplication,
    }

    def __init__(self, *args, **kwargs):
      super(SimpleSwitch, self).__init__(*args, **kwargs)
      config = {
        'user' : 'root',
        'password' : 'root',
        'host' : 'db',
        'port' : '3306',
        'database' : 'emulator'
      }
      self.connection = mysql.connector.connect(**config)
      self.connection.autocommit = True
      self.cursor = self.connection.cursor(buffered=True)
      self.mac_to_port = {}
      wsgi = kwargs['wsgi']
      wsgi.register(GUIServerController)


    def add_flow(self, datapath, in_port, dst, src, actions):
      ofproto = datapath.ofproto

      match = datapath.ofproto_parser.OFPMatch(
        in_port=in_port,
        dl_dst=haddr_to_bin(dst), dl_src=haddr_to_bin(src))

      mod = datapath.ofproto_parser.OFPFlowMod(
        datapath=datapath, match=match, cookie=0,
        command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
        priority=ofproto.OFP_DEFAULT_PRIORITY,
        flags=ofproto.OFPFF_SEND_FLOW_REM, actions=actions)
      datapath.send_msg(mod)

    def dpid_state_from_mininet(self, name):
      if 'h' in name:
        return (0,0,0)
      dpid = dpid_to_str(int(name[1:]))


      query = "SELECT * FROM charge_state WHERE dpid = \'%s\'" % dpid 
      self.cursor.execute(query)
      rv = self.cursor.fetchall() 
      return rv[0]

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
      msg = ev.msg
      datapath = msg.datapath
      ofproto = datapath.ofproto

      pkt = packet.Packet(msg.data)
      eth = pkt.get_protocol(ethernet.ethernet)

      query = "SELECT * FROM charge_state" 
      self.cursor.execute(query)
      empty_charge_state = len(self.cursor.fetchall()) == 0

      query = "SELECT * FROM mac_to_dpid" 
      self.cursor.execute(query)
      empty_mac_to_dpid = len(self.cursor.fetchall()) == 0

      if eth.ethertype == ether_types.ETH_TYPE_LLDP or empty_charge_state or empty_mac_to_dpid:
        # ignore lldp packet
        return
      dst = eth.dst
      src = eth.src

      dpid = datapath.id
      self.mac_to_port.setdefault(dpid, {})

      self.logger.info("packet in %s %s %s %s", dpid, src, dst, msg.in_port)

      self.cursor.execute('SELECT * FROM topology ORDER BY id;')
      links = self.cursor.fetchall()

      nodes = set()
      g = Graph()
      for pos, node_a, node_b in links:
        nodes.add(node_a)
        nodes.add(node_b)
        tempo_testo = self.dpid_state_from_mininet(node_a)
        _, node_a_charge, _  = self.dpid_state_from_mininet(node_a)
        _, node_b_charge, _  = self.dpid_state_from_mininet(node_b)
        g.add_edge(node_a, node_b, 1/node_a_charge if node_a_charge != 0 else 0)
        g.add_edge(node_b, node_a, 1/node_b_charge if node_b_charge != 0 else 0)

      for node in nodes:
        g.add_node(node)

      cur_node = 's%d' % dpid
      _, path = dijsktra(g, cur_node)
      self.cursor.execute('SELECT dpid FROM mac_to_dpid WHERE mac_addr = \'%s\'' % eth.dst)
      dst_node = self.cursor.fetchall()
      print "YAY ", dst_node
      if 'h' not in dst_node:
        dst_node = 's%d' % int(dst_node)

      while path[dst_node] != cur_node:
        dst_node = path[dst_node]

      result_port = 0
      for _, node_a, node_b in links:
        if cur_node in [node_a, node_b]:
          result_port += 1
          if dst_node in [node_a, node_b]:
            break
      out_port = result_port

      # learn a mac address to avoid FLOOD next time.
      #self.mac_to_port[dpid][src] = msg.in_port

      #if dst in self.mac_to_port[dpid]:
      #  out_port = self.mac_to_port[dpid][dst]
      #else:
      #  out_port = ofproto.OFPP_FLOOD

      query = "SELECT * FROM charge_state WHERE dpid = %s" % dpid 
      self.cursor.execute(query)
      hdrs = [x[0] for x in self.cursor.description]
      rv = self.cursor.fetchall() 
      _, charge, _  = rv[0] 
      t = 4.81 # time of sending
      # INITIAL VALUE
      new_charge = charge - ((t*random.randint(19, 21)) + (t*random.randint(104, 114)))
      query = "UPDATE charge_state SET charge = %s WHERE dpid = %s"
      self.cursor.execute(query, (new_charge, dpid))

      ts = time.time()
      timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
      query = "INSERT INTO charge_events (dpid, charge, ts) VALUES (%s, %s, %s)"
      self.cursor.execute(query, (dpid, new_charge, timestamp))

      query = "INSERT INTO send_events (dpid, from_mac, to_mac, from_port, to_port, ts) VALUES (%s, %s, %s, %s, %s, %s)"
      self.cursor.execute(query, (dpid, src, dst, msg.in_port, out_port, timestamp))

      self.connection.commit()

      actions = [datapath.ofproto_parser.OFPActionOutput(out_port)]

      # install a flow to avoid packet_in next time
      if out_port != ofproto.OFPP_FLOOD:
        self.add_flow(datapath, msg.in_port, dst, src, actions)

      data = None
      if msg.buffer_id == ofproto.OFP_NO_BUFFER:
        data = msg.data

      out = datapath.ofproto_parser.OFPPacketOut(
        datapath=datapath, buffer_id=msg.buffer_id, in_port=msg.in_port,
        actions=actions, data=data)
      datapath.send_msg(out)

    def _update_charge(self, dpid, is_receiver):
      return res

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def _port_status_handler(self, ev):
      msg = ev.msg
      reason = msg.reason
      port_no = msg.desc.port_no

      ofproto = msg.datapath.ofproto
      if reason == ofproto.OFPPR_ADD:
        self.logger.info("port added %s", port_no)
      elif reason == ofproto.OFPPR_DELETE:
        self.logger.info("port deleted %s", port_no)
      elif reason == ofproto.OFPPR_MODIFY:
        self.logger.info("port modified %s", port_no)
      else:
        self.logger.info("Illeagal port state %s %s", port_no, reason)

class GUIServerController(ControllerBase):
    def __init__(self, req, link, data, **config):
      super(GUIServerController, self).__init__(req, link, data, **config)
      path = "%s/html/" % PATH
      self.static_app = DirectoryApp(path)

    @route('topology', '/{filename:[^/]*}')
    def static_handler(self, req, **kwargs):
      if kwargs['filename']:
        req.path_info = kwargs['filename']
      return self.static_app(req)

app_manager.require_app('ryu.app.rest_topology')
app_manager.require_app('ryu.app.ws_topology')
app_manager.require_app('ryu.app.ofctl_rest')

