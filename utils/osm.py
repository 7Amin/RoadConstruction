import osmium as osm
import pandas as pd


class TimelineHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.elemtimeline = []

    def node(self, n):
        try:
            self.elemtimeline.append(["node",
                                      n.id,
                                      n.version,
                                      n.visible,
                                      n.location.lat,
                                      n.location.lon,
                                      None,
                                      pd.Timestamp(n.timestamp),
                                      n.uid,
                                      n.changeset,
                                      #'-'.join(n.tags) if n.tags else '',
                                      len(n.tags)])
        except Exception as error:
            print(error)

    def way(self, w):
        try:
            self.elemtimeline.append(["way",
                                      w.id,
                                      w.version,
                                      w.visible,
                                      0,
                                      0,
                                      # '-'.join(w.nodes),
                                      pd.Timestamp(w.timestamp),
                                      w.uid,
                                      w.changeset,
                                      #'-'.join(w.tags) if w.tags else '',
                                      len(w.tags)
                                      ])
        except Exception as error:
            print(error)




print(1)
tlhandler = TimelineHandler()
print(2)
tlhandler.apply_file("/home/amin/CETI/RoadConstruction/OSM/USA/States/delaware-latest.osm.pbf")
colnames = ['type', 'id', 'version', 'visible', 'lat', 'lon', 'nodes', 'ts', 'uid', 'chgset', 'ntags']
print(5)
elements = pd.DataFrame(tlhandler.elemtimeline, columns=colnames)
print(6)
c = elements.head(100)
print(c)
print(7)

ways = elements[elements['type'] == 'way']
print(ways)
print(ways.count())
print(10)
nodes = elements[elements['type'] == 'node']
print(nodes)
print(nodes.count())
