import glob
import pandas as pd
import bz2
import json


files = [filename for filename in glob.glob('..\\..\\data\\peeringdb\\json_dumps\\peeringdb*.json.bz2*') if '2019' in filename or '2017' in filename or '2018' in filename]

for f in files:
    with bz2.open(f, 'rt') as dump:
        pdb = json.load(dump)
    if 'peerParticipantsPublics' in pdb.keys() and 'peerParticipantsPublics' in pdb.keys():
            continue
    print('Mapping',f,'...')
    if '2017' in f or '2018' in f:
        peerParticipantsPublics = pd.DataFrame(pdb['peeringdb_network_ixlan'])
        mgmtPublics = pd.DataFrame(pdb['peeringdb_ix'])
    if '2019' in f:
        mgmtPublics = pd.DataFrame(pdb['ix']['data'])
        peerParticipantsPublics = pd.DataFrame(pdb['netixlan']['data'])
        
    peerParticipantsPublics.rename(columns = {'asn':'local_asn', 'net_id':'participant_id', 'ixlan_id':'public_id'}, errors='raise', inplace = True)
    
    pdb['peerParticipantsPublics'] = peerParticipantsPublics.to_dict()
    pdb['mgmtPublics'] = mgmtPublics.to_dict()

    # removing _2 from json file's name, makes it aligned with the format tb_common code expects
    f = f.replace('peeringdb_2_dump','peeringdb_dump')
    
    f = f.replace('.json.bz2_','.json.bz2')
    with bz2.open(f, 'wt') as f:
        json.dump(pdb, f)