from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_0
from ryu.lib.mac import haddr_to_bin
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types

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
      self.cursor = self.connection.cursor()
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

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
      msg = ev.msg
      datapath = msg.datapath
      ofproto = datapath.ofproto

      pkt = packet.Packet(msg.data)
      eth = pkt.get_protocol(ethernet.ethernet)

      if eth.ethertype == ether_types.ETH_TYPE_LLDP:
        # ignore lldp packet
        return
      dst = eth.dst
      src = eth.src

      dpid = datapath.id
      self.mac_to_port.setdefault(dpid, {})

      self.logger.info("packet in %s %s %s %s", dpid, src, dst, msg.in_port)

      # learn a mac address to avoid FLOOD next time.
      self.mac_to_port[dpid][src] = msg.in_port

      if dst in self.mac_to_port[dpid]:
        out_port = self.mac_to_port[dpid][dst]
      else:
        out_port = ofproto.OFPP_FLOOD

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

