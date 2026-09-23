import copy
import unittest
from bitstamp_live_orders_audit import audit

class AuditTests(unittest.TestCase):
    def capture(self):
        seed={'microtimestamp':'1000','bids':[['100','2','1'],['99','4','2']],'asks':[['101','3','3']]}
        raw={'start_ns':10,'end_ns':100,'seeds':{'test':seed},'messages':[], 'final_snapshots':{'test':dict(seed,microtimestamp='9000')}}
        for i,(ts,action,oid,p,q) in enumerate([(2000,'order_created','4','98','1'),(3000,'order_created','5','100','1'),(4000,'order_deleted','5','100','1'),(5000,'order_deleted','4','98','1')]):
            raw['messages'].append({'recv_ns':20+i,'message':{'channel':'live_orders_test','event':action,'event_id':str(i+1),'pre_event_id':str(i),'order_source':'orderbook','data':{'microtimestamp':str(ts),'id_str':oid,'order_type':0,'price_str':p,'amount_str':q}}})
        return raw
    def test_top_sizes_and_deep_events(self):
        c=audit(self.capture())['test']['counts']
        self.assertEqual(c['valid_per_event_changes'],2)
        self.assertEqual(c['valid_per_event_size_only_changes'],2)
        self.assertEqual(c['timestamp_grouped_changes'],2)
        self.assertEqual(c['final_full_order_map_match'],1)
    def test_same_millisecond_is_not_one_event(self):
        raw=self.capture();raw['messages'][2]['message']['data']['microtimestamp']='3000'
        c=audit(raw)['test']['counts']
        self.assertEqual(c['valid_per_event_changes'],2)
        self.assertEqual(c['timestamp_grouped_changes'],0)
    def test_missing_event_breaks_chain_and_changes_final_book(self):
        raw=self.capture();del raw['messages'][2]
        c=audit(raw)['test']['counts']
        self.assertEqual(c['chain_breaks'],1)
        self.assertEqual(c['final_full_order_map_match'],0)
        self.assertEqual(c['final_order_id_symmetric_difference'],1)
    def test_periodic_snapshot_value_mismatch(self):
        raw=self.capture();mid=copy.deepcopy(raw['seeds']['test']);mid['microtimestamp']='5500';mid['bids'][0][1]='9'
        raw['checkpoints']={'test':[mid]}
        r=audit(raw)['test']['rest_checkpoints'][0]
        self.assertFalse(r['full_order_map_match'])
        self.assertEqual(r['order_value_differences'],1)

if __name__=='__main__': unittest.main()
