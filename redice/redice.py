import uuid
from hashlib import md5
import json
# import time

from errors import RedIceErrors
from shared import RedIceShared
# import redis
from redis.sentinel import Sentinel, MasterNotFoundError



class RedIce():
    """docstring for ."""
    def __init__(self):
        self.SOCK_TIMEOUT = 1

        self.redice_errors = RedIceErrors()
        self.redice_shared = RedIceShared(self.redice_errors)

        self.sentinel_conn = None
        self.redis_conn = None
        self.cluster_name = None


    def _get_cluster_name(self):
        return self.cluster_name

    def _key_is_has(self, ids_class, ids_type, ids_value, expect=False):
        errmsg = {
            False: 'already in use for other',
            True: 'not found for'
            }

        result_check = self.redis_conn.exists(
            '%s:registry:%s:%s:%s'%(self._get_cluster_name(),
                           ids_class, ids_type, ids_value.lower())
        )

        if expect is result_check:
            return True
        else:
            self.redice_errors.error_reg(
                '%sIsHas'%(ids_type.upper()),
                '%s %s %s %s'%(
                    ids_type.upper(), ids_value, errmsg[expect], ids_class))
            return False


    def _hash_is_has(self, key, val, expect=False):
        errmsg = {
            False: 'already in use for other',
            True: 'not found for'
            }

        result_check = self.redis_conn.hexists(key, val)

        if expect is result_check:
            return True
        else:
            self.redice_errors.error_reg(
                'HashIsHas',
                '%s -> %s: %s'%(
                    key, val, errmsg[expect]))
            return False

    def _get_uuid_by_name(self, ids_class, name):
        #To Do get uuid from Redis
        uuid = self.redis_conn.get(
            '%s:registry:%s:name:%s'%(
                self._get_cluster_name(), ids_class, name.lower())
            )
        if not uuid:
            self.redice_errors.error_reg(
                'UUIDByName',
                'Name %s not found'%(
                    name))
            return None
        return uuid.decode('utf-8')

    def _get_meta_by_uuid(self, ids_class, uuid):
        if not self.redis_conn.exists(
            '%s:registry:%s:uuid:%s'%(
                self._get_cluster_name(), ids_class, uuid)):
            self.redice_errors.error_reg(
                'GetMetaByUUID',
                'Meta by %s not found'%(
                    uuid))
            return None

        meta = self.redis_conn.get(
            '%s:registry:%s:uuid:%s'%(
                self._get_cluster_name(), ids_class, uuid
            ))
        if not meta:
            self.redice_errors.error_reg(
                'GetMetaByUUID',
                'Meta by %s not found'%(
                    uuid))
            return None
        return json.loads(meta.decode('utf-8'))

    def _get_keys_for(self, q_str, seg_index=-1):
        vals = []
        keys = self.redis_conn.keys(q_str)
        if keys:
            for k in keys:
                k = k.decode('utf-8')
                if seg_index > -1:
                    v = k.split(':')[seg_index]
                else:
                    v = k
                vals.append(v)
            return vals
        else:
            return None

    def _identify_uuid(self, ids_class, ids_value):
        ids_type = self.redice_shared.uuid4_or_name(ids_value)

        if ids_type == 'uuid':
            uuid = ids_value
        elif ids_type == 'name':
            uuid = self._get_uuid_by_name(ids_class, ids_value)
        else:
            return None

        if uuid:
            if self._key_is_has(ids_class, 'uuid', uuid, True):
                return uuid
        else:
            return None

    def _redis_connect(self, timeout_factor=1):
        try:
            self.redis_conn = self.sentinel_conn.master_for(
                self._get_cluster_name(),
                socket_timeout=self.SOCK_TIMEOUT*float(timeout_factor))
            return True
        except Exception as e:
            self.redice_errors.error_reg(
                'RedisConnection: %s'%(e))
        return True

    def _get_all_uuids(self, class_ids):
        r = []
        for v in self._get_keys_for(
            '%s:registry:%s:uuid:*'%(
                self._get_cluster_name(), class_ids), seg_index=4):
            r.append(v)
        return r

    # def _get_all_maps_uuids(self):
    #     r = []
    #     for v in self._get_keys_for(
    #         '%s:registry:maps:name:*'%(
    #             self._get_cluster_name()), seg_index=4):
    #         maps_list.append(v)
    #     return r

    # def _get_all_shards_uuids(self):
    #     shards_list = []
    #     for shard_key in self.get_keys_for_q(
    #         '%s:registry:shards:name:*'%(self._get_cluster_name())):
    #         shards_list.append(
    #             self.get_shard_info(self.redis_conn.get(shard_key).decode('utf-8')))
    #     return maps_list

    def _remove_empty_list(self, key):
        if self.redis_conn.exists(key):
            if int(self.redis_conn.llen(key)) == 0:
                self.redis_conn.delete(key)

    def get_shard_map(self, map_name):
        r = {}
        hashmap = self.redis_conn.hgetall(
            '%s:maps:%s:hashsmaps'%(self._get_cluster_name(), map_name))
        if hashmap:
            for ha, sh in hashmap.items():
                r[ha.decode('utf-8')] = int(sh.decode('utf-8'))
            return r
        else:
            return None

    #API????? dubles self._get_meta_by_uuid
    # def get_map_info(self, map_uuid):
    #     results_list = []
    #     meta_info = self._get_meta_by_uuid('maps', map_uuid)
    #     return meta_info


    # def get_keys_for_q(self, q_str):
    #     keys = []
    #     res = self.redis_conn.keys(q_str)
    #     if res:
    #         for k in res:
    #             keys.append(k.decode('utf-8'))
    #         return keys
    #     else:
    #         return None

    def get_errors(self):
        return self.redice_errors.get_errors()

    def connect(self, sentinels, root_name):
        for sentinel_server in sentinels:
            addr = sentinel_server['server'].split(':')
            s, p = addr[0].strip(), int(addr[1].strip())
            self.cluster_name = '%s.%s'%(root_name.lower(), 'redice')
            m_list = None
            try:
                self.sentinel_conn = Sentinel(
                    [(s, p)], socket_timeout=self.SOCK_TIMEOUT)

                m_list = self.sentinel_conn.discover_master(
                    self._get_cluster_name())
                s_list = self.sentinel_conn.discover_slaves(
                    self._get_cluster_name())

                self._redis_connect()
                # self.redis_conn = self.sentinel_conn.master_for(
                #     self._get_cluster_name(),
                #     socket_timeout=self.SOCK_TIMEOUT)

                print('masters: ', m_list)
                print('slaves: ', s_list)
            except Exception as e:
                self.redice_errors.error_reg(
                    'SentinelConnection: %s'%(sentinel_server['server']), e)
            else:
                break


        if m_list:
            return True
        else:
            return False

    def create_map(self, map_name, map_size, map_blocks, map_uuid=None):
        results_list = []

        slots = {1: 16, 2: 256, 3: 4096, 4: 16384, 5: 65536}
        max_slots = slots[map_size]
        map_name = map_name.lower()

        if map_uuid is None:
            map_uuid=str(uuid.uuid4())

        # check uuid unique
        results_list.append(self._key_is_has('maps', 'uuid', map_uuid))

        # check name unique
        results_list.append(self._key_is_has('maps', 'name', map_name))

        if False in results_list:
            return False

        if (map_blocks > max_slots) or (map_blocks < 1 ):
            self.redice_errors.error_reg(
                'HashMapCreate',
                'map_blocks=%d: but must be between 1 to %d, for map_size=%d(%d slots)'%(
                    map_blocks, max_slots, map_size, max_slots))
            return False

        cur_map = {
            'name': map_name,
            'slots': max_slots,
            'blocks': map_blocks,
            'uuid': map_uuid,
            'size': map_size
            }

        if not self._redis_connect(map_size):
            return False

        pipe = self.redis_conn.pipeline()
        shard_num = 1
        session_uuid = str(uuid.uuid4())

        md5_block = md5(str('%s%d%s'%(
            map_name, shard_num, session_uuid)).encode('utf-8')).hexdigest()

        avr_block = max_slots / map_blocks

        if avr_block - int(avr_block) == 0:
            _div = False
        else:
            _div = True

        block_size = int(avr_block)

        for slot in range(max_slots):
            if block_size <= 0:
                shard_num += 1
                if shard_num > map_blocks:
                    shard_num = map_blocks
                md5_block = md5(str('%s%d%s'%(
                    map_name, shard_num, session_uuid)).encode('utf-8')).hexdigest()
                if _div:
                    try:
                        _avr = (max_slots-slot) / ((map_blocks-shard_num)+1)
                    except ZeroDivisionError as e:
                        _avr = 1
                    if _avr - int(_avr) > .5:
                        block_size = int(_avr)+1
                    else:
                        block_size = int(_avr)
                else:
                    block_size = int(avr_block)
            block_size -= 1

            # DEBUG
            print(slot, hex(slot), map_name, shard_num, md5_block, (slot % int(max_slots / map_blocks)))

            cur_hash = hex(slot)
            pipe.hset(
                '%s:maps:%s:hashsmaps'%(self._get_cluster_name(), map_name),
                cur_hash,
                md5_block)
            pipe.rpush(
                '%s:maps:%s:blocks:%s'%(self._get_cluster_name(), map_name, md5_block),
                cur_hash
            )
            pipe.set(
                '%s:registry:blocks:md5:%s'%(self._get_cluster_name(), md5_block),
                map_name
            )
        pipe.set(
            '%s:registry:maps:uuid:%s'%(self._get_cluster_name(), map_uuid),
            json.dumps(cur_map)
        )
        pipe.set('%s:registry:maps:name:%s'%(self._get_cluster_name(), map_name),
                 map_uuid)

        try:
            q_res = pipe.execute()
            if False in q_res:
                self.redice_errors.error_reg(
                    'CreateMap',
                    'Create map name transaction not completed: %s'%(q_res))
                return False
            else:
                return True

        except Exception as e:
            self.redice_errors.error_reg(
                'CreateMap',
                'Create map name transaction error: %s'%(e))

        return False

    def modify_map(self, obj, map_name):
        results_list = []
        map_uuid = self._identify_uuid('maps', obj)

        if not map_uuid:
            return False
        # Check new values
        if not True in [bool(map_name)]:
            self.redice_errors.error_reg(
                'ModifyMap',
                'Not specified what to change in map: %s'%(
                    obj))
            return False

        map_name = map_name.lower()

        # Check all keys
        meta_info = self._get_meta_by_uuid('maps', map_uuid)

        if not self._redis_connect(meta_info['size']):
            return False

        results_list.append(bool(meta_info))

        if bool(map_name) and meta_info['name'] != map_name:
            results_list.append(self.redice_shared.name_validate(map_name))
            results_list.append(self._key_is_has('maps', 'name', map_name, expect=False))
        else:
            self.redice_errors.error_reg(
                'ModifyMap',
                'New map name: %s alredy exists'%(
                    map_name))
            results_list.append(False)

        if not False in results_list:
            try:
                # ToDo: Add rename transaction to LUA
                pipe = self.redis_conn.pipeline()

                pipe.rename(
                    '%s:maps:%s:hashsmaps'%(self._get_cluster_name(), meta_info['name']),
                    '%s:maps:%s:hashsmaps'%(self._get_cluster_name(), map_name))

                pipe.rename(
                    '%s:registry:maps:name:%s'%(self._get_cluster_name(),
                                                meta_info['name']),
                    '%s:registry:maps:name:%s'%(self._get_cluster_name(), map_name))

                for sh_num in self._get_keys_for(
                    '%s:maps:%s:blocks:*'%(
                        self._get_cluster_name(), meta_info['name'])):
                    pipe.rename(
                        sh_num,
                        '%s:maps:%s:blocks:%s'%(self._get_cluster_name(),
                                                map_name,
                                                sh_num.split(':')[4]))
                    pipe.set(
                        '%s:registry:blocks:md5:%s'%(
                            self._get_cluster_name(),
                            sh_num.split(':')[4]),
                        map_name)

                meta_info['name'] = map_name

                pipe.set(
                    '%s:registry:maps:uuid:%s'%(self._get_cluster_name(), map_uuid),
                    json.dumps(meta_info))

                q_res = pipe.execute()
                if False in q_res:
                    self.redice_errors.error_reg(
                        'ModifyMap',
                        'Change map transaction not completed: %s'%(
                            q_res))
                    return False
                else:
                    return True

            except Exception as e:
                self.redice_errors.error_reg(
                    'ModifyMap',
                    'Change map transaction error: %s'%(
                        e))
        return False


    def delete_map(self, obj):
        results_list = []
        map_uuid = self._identify_uuid('maps', obj)

        if not map_uuid:
            return False

        meta_info = self._get_meta_by_uuid('maps', map_uuid)
        results_list.append(bool(meta_info))

        if not self._redis_connect(meta_info['size']):
            return False

        if not False in results_list:

            pipe = self.redis_conn.pipeline()

            pipe.delete(
                '%s:maps:%s:hashsmaps'%(
                    self._get_cluster_name(), meta_info['name']))

            for sh_num in self._get_keys_for(
                '%s:maps:%s:blocks:*'%(
                    self._get_cluster_name(), meta_info['name'])):
                pipe.delete(sh_num)
                pipe.delete(
                    '%s:registry:blocks:md5:%s'%(
                        self._get_cluster_name(),
                        sh_num.split(':')[4]))

            pipe.delete(
                '%s:registry:maps:name:%s'%(self._get_cluster_name(),
                                            meta_info['name']))

            pipe.delete(
                '%s:registry:maps:uuid:%s'%(self._get_cluster_name(), map_uuid))

            try:
                q_res = pipe.execute()
                if False in q_res:
                    self.redice_errors.error_reg(
                        'DeleteMap',
                        'Delete map transaction not completed: %s'%(
                            q_res))
                    return False
                else:
                    return True

            except Exception as e:
                self.redice_errors.error_reg(
                    'DeleteMap',
                    'Delete map transaction error: %s'%(
                        e))

        return False

    def blocks_list(self, map_uuid):
        blocks_list = []
        meta_info = self._get_meta_by_uuid('maps', map_uuid)
        for key_block in self._get_keys_for(
            '%s:maps:%s:blocks:*'%(
                self._get_cluster_name(), meta_info['name'])):
            md5_block = key_block.split(':')[4]
            cur_shard = self.redis_conn.hget(
                '%s:registry:blocks'%(self._get_cluster_name()),
                md5_block)
            if not cur_shard:
                cur_shard = 'not assigned'
            else:
                cur_shard = cur_shard.decode('utf-8')
            blocks_list.append({
                'id': md5_block,
                'shard': cur_shard,
                'slots': int(self.redis_conn.llen(key_block))
            })
        return blocks_list

    def _blocks_print(self, blocks_list):
        if not blocks_list:
            print('Error! This map without blocks')
        i = 0
        for block in blocks_list:
            i += 1
            print('{0:4s} {1}'.format(
                '', '%d. %d slots of %s -> %s'%(
                    i, block['slots'], block['id'], block['shard'])))


    def _maps_print(self, maps_list, short):
        if not maps_list:
            print('No registered maps for %s'%(self._get_cluster_name()))
        i = 0
        for info_map in maps_list:
            i += 1
            if short:
                print('{0:2s} {1} {2} (type={3} ({4} Slots) blocks={5})'.format(
                    '%d.'%(i),
                    info_map['uuid'], info_map['name'], info_map['size'],
                    info_map['slots'], info_map['blocks']
                ))
            else:
                print('\n{0:2s} {1} info:'.format(
                    '%d.'%(i), 'Hash map {%s}'%(info_map['name'])))
                print('{0:3s}General:'.format(''))
                print('{0:4s}{1:12s}{2}'.format('', 'Name:', info_map['name']))
                print('{0:4s}{1:12s}{2}'.format('', 'UUID:', info_map['uuid']))
                print('\n{0:3s}Options:'.format(''))
                print('{0:4s}{1:12s}{2:6s}'.format(
                    '', 'Map size:', 'Type %d (%d Slots)'%(
                            info_map['size'], info_map['slots'])))
                print('{0:4s}{1:12s}{2}'.format('', 'Blocks:', info_map['blocks']))
                print('\n{0:3s}Blocks to Shards:'.format(''))
                self._blocks_print(self.blocks_list(info_map['uuid']))

    def _shards_print(self, shards, group_by, short):
        # print('shards_list: ', shards)
        if not shards:
            print('No registered shards for %s'%(self._get_cluster_name()))
        i = 0
        print('Shards list of {%s} cluster group by {%s}'%(
            self._get_cluster_name(), group_by))
        for k, v in shards.items():
            print('\n\n')
            print('{0:{fill}{align}80}'.format(
                ' %s '%(k.upper()), fill='-', align='^'))
            for info_shard in v:
                i+=1
                if short:
                    print('{0:1s} {1:3s} {2:12s} {3}'.format(
                        '',
                        '%d.'%(i),
                        info_shard['name'],
                        '%s -> %s'%(info_shard['uuid'],info_shard['block'])
                    ))
                else:
                    print('\n\n{0:1s} {1:3s} {2:12s}'.format(
                        '',
                        '%d.'%(i),
                        info_shard['name']
                    ))
                    print('{0:3s}General:'.format(''))
                    print('{0:4s}{1:12s}{2}'.format('', 'Name:', info_shard['name']))
                    print('{0:4s}{1:12s}{2}'.format('', 'UUID:', info_shard['uuid']))
                    print('{0:4s}{1:12s}{2}'.format('', 'Assigned:', info_shard['block']))
                    if info_shard['block']:
                        print('{0:4s}{1:12s}{2}'.format('', 'Slots:', len(info_shard['slot-list'])))
                        print('{0:3s}Slots list:'.format(''))
                        l = 5
                        for slot in info_shard['slot-list']:
                            l-=1
                            slot_num = int(slot.decode('utf-8'), 0)
                            print('{0:10s}'.format(
                                '{0:10d}{1:#6x}'.format(slot_num, slot_num)), end=' ')
                            if l <= 0:
                                l = 5
                                print()
                    print()


    def info_map(self, obj, short=False):
        results_list = []
        map_uuid = self._identify_uuid('maps', obj)
        info_map = self._get_meta_by_uuid('maps', map_uuid)
        if info_map:
            self._maps_print([info_map], short)
            return True
        else:
            self.redice_errors.error_reg(
                'MapInfo',
                'Map info not assigned to %s'%(
                    obj))
            return False


    def maps_list(self, short=False):
        maps_list = []
        for map_uuid in self._get_all_uuids('maps'):
            info = self._get_meta_by_uuid('maps', map_uuid)
            maps_list.append(info)

        if maps_list:
            self._maps_print(maps_list, short)
            return True
        return False

    def get_shards_info(self, shard_uuid):
        info = self._get_meta_by_uuid('shards', shard_uuid)
        map_name = self.redis_conn.get(
            '%s:registry:blocks:md5:%s'%(
                self._get_cluster_name(),
                info['block']))
        if map_name:
            # Additional meta info
            info['map'] = map_name.decode('utf-8')
            info['slot-list'] = self.redis_conn.lrange(
                '%s:maps:%s:blocks:%s'%(
                    self._get_cluster_name(),
                    info['map'],
                    info['block']), 0, -1)
        else:
            info['map'] = 'not assigned'
        return info

    def shards_list(self, group_by, short=False):
        shards = {}
        for shard_uuid in self._get_all_uuids('shards'):
            info = self.get_shards_info(shard_uuid)
            if not info[group_by] in shards:
                shards[info[group_by]] = []
            shards[info[group_by]].append(info)

        if shards:
            self._shards_print(shards, group_by, short)
            return True
        return False

    def add_shard(self, shard_name, block_md5, shard_group, shard_uuid):
        results_list = []

        shard_name = shard_name.lower()

        if not shard_group:
            shard_group = 'main'
        shard_group = shard_group.lower()

        if shard_uuid is None:
            shard_uuid=str(uuid.uuid4())


        # check uuid unique
        results_list.append(self._key_is_has('shards', 'uuid', shard_uuid))

        # check name unique
        results_list.append(self._key_is_has('shards', 'name', shard_name))

        # check block_md5 for exists
        if block_md5:
            results_list.append(self._key_is_has(
                'blocks', 'md5', block_md5, expect=True))

        print(results_list)
        # check block for already use
        results_list.append(self._hash_is_has(
            '%s:registry:blocks'%(self._get_cluster_name()),
            block_md5, expect=False))

        if False in results_list:
            return False

        shard_meta = {
            'name': shard_name,
            'group': shard_group,
            'block': block_md5,
            'uuid':shard_uuid,
            'nodes': []
            }

        pipe = self.redis_conn.pipeline()

        pipe.rpush(
            '%s:shards:%s'%(self._get_cluster_name(), shard_group),
            shard_name
        )

        pipe.set(
            '%s:registry:shards:uuid:%s'%(self._get_cluster_name(), shard_uuid),
            json.dumps(shard_meta)
        )

        pipe.set(
            '%s:registry:shards:name:%s'%(self._get_cluster_name(), shard_name),
            shard_uuid
        )

        if block_md5:
            pipe.hset(
                '%s:registry:blocks'%(self._get_cluster_name()),
                block_md5,
                shard_uuid)

        try:
            q_res = pipe.execute()
            print('TRANSACTION RES: ', q_res)
            if False in q_res:
                self.redice_errors.error_reg(
                    'AddShard',
                    'Add shard transaction not completed: %s'%(q_res))
                return False
            else:
                return True

        except Exception as e:
            self.redice_errors.error_reg(
                'AddShard',
                'Add shard transaction error: %s'%(e))

        return False


    def modify_shard(self, obj, block_md5, shard_name, shard_group):
        results_list = []
        shard_uuid = self._identify_uuid('shards', obj)

        if not shard_uuid:
            return False
        # Check new values
        if not True in [bool(shard_name), bool(shard_group), bool(block_md5)]:
            self.redice_errors.error_reg(
                'ModifyShard',
                'Not specified what to change in shard: %s'%(
                    obj))
            return False

        meta_info = self._get_meta_by_uuid('shards', shard_uuid)


        results_list.append(bool(meta_info))

        to_change = {
            'name': False,
            'group': False,
            'block': False
        }

        if bool(shard_name):
            shard_name = shard_name.lower()
        else:
            shard_name = meta_info['name']

        if bool(shard_group):
            shard_group = shard_group.lower()
        else:
            shard_group = meta_info['group']

        if bool(block_md5):
            block_md5 = block_md5.lower()
        else:
            block_md5 = meta_info['block']


        if bool(shard_name) and meta_info['name'] != shard_name:
            res_valid = self.redice_shared.name_validate(shard_name)
            results_list.append(res_valid)
            if res_valid:
                to_change['name'] = True
                results_list.append(
                    self._key_is_has(
                        'shards', 'name', shard_name, expect=False))

        if bool(shard_group) and meta_info['group'] != shard_group:
            to_change['group'] = True
            results_list.append(self.redice_shared.name_validate(shard_group))

        if bool(block_md5) and meta_info['block'] != block_md5:
            res_valid = self.redice_shared.name_validate(block_md5)
            results_list.append(res_valid)
            if res_valid:
                to_change['block'] = True
                results_list.append(
                    self._key_is_has(
                        'blocks', 'md5', block_md5, expect=True))
                results_list.append(
                    self._hash_is_has(
                        '%s:registry:blocks'%(self._get_cluster_name()),
                        block_md5,
                        expect=False
                    )
                )


        if False in results_list:
            return False

        if not True in [to_change['group'], to_change['name'], to_change['block']]:
            return False

        pipe = self.redis_conn.pipeline()

        if to_change['group'] or to_change['name']:

            #Remove shard name from group list
            pipe.lrem(
                '%s:shards:%s'%(self._get_cluster_name(), meta_info['group']),
                0,
                meta_info['name']
            )

            # Add new shard group anf name
            pipe.rpush(
                '%s:shards:%s'%(self._get_cluster_name(), shard_group),
                shard_name
            )

            if to_change['name']:
                pipe.rename(
                    '%s:registry:shards:name:%s'%(self._get_cluster_name(),
                                                  meta_info['name']),
                    '%s:registry:shards:name:%s'%(self._get_cluster_name(), shard_name))

        if to_change['block']:
            pipe.hdel(
                '%s:registry:blocks'%(self._get_cluster_name()),
                meta_info['block'])
            pipe.hset(
                '%s:registry:blocks'%(self._get_cluster_name()),
                block_md5,
                shard_uuid)

        meta_info['group'] = shard_group
        meta_info['name'] = shard_name
        meta_info['block'] = block_md5

        pipe.set(
            '%s:registry:shards:uuid:%s'%(self._get_cluster_name(), shard_uuid),
            json.dumps(meta_info))

        try:
            print('BEGIN TR: ')
            rrr = pipe.execute()
            print('TR: ', rrr)
            # ToDo check all results or use LUA
            # q_res = pipe.execute()
            # if False in q_res:
            #     self.redice_errors.error_reg(
            #         'ModifyShard',
            #         'Modify shard transaction not completed: %s'%(q_res))
            #     return False
            # else:
            #   return True
            if to_change['group']:
                self._remove_empty_list(
                    '%s:shards:%s'%(self._get_cluster_name(), meta_info['group']))
            return True

        except Exception as e:
            self.redice_errors.error_reg(
                'ModifyShard',
                'Modify shard transaction error: %s'%(e))

        return False

    def release_shard(self, obj):
        results_list = []

        shard_uuid = self._identify_uuid('shards', obj)
        if not shard_uuid:
            return False

        meta_info = self._get_meta_by_uuid('shards', shard_uuid)
        results_list.append(bool(meta_info))

        if False in results_list:
            return False

        pipe = self.redis_conn.pipeline()
        pipe.hdel(
            '%s:registry:blocks'%(self._get_cluster_name()),
            meta_info['block'])

        meta_info['block'] = None

        pipe.set(
            '%s:registry:shards:uuid:%s'%(self._get_cluster_name(), shard_uuid),
            json.dumps(meta_info))

        try:
            pipe.execute()
            return True
        except Exception as e:
            self.redice_errors.error_reg(
                'ReleaseShard',
                'Release shard transaction error: %s'%(e))

        return False



    def delete_shard(self, obj):
        results_list = []

        shard_uuid = self._identify_uuid('shards', obj)
        if not shard_uuid:
            return False

        meta_info = self._get_meta_by_uuid('shards', shard_uuid)
        results_list.append(bool(meta_info))

        if False in results_list:
            return False

        pipe = self.redis_conn.pipeline()
        pipe.hdel(
            '%s:registry:blocks'%(self._get_cluster_name()),
            meta_info['block'])
        pipe.lrem(
            '%s:shards:%s'%(self._get_cluster_name(), meta_info['group']),
            0,
            meta_info['name']
        )
        pipe.delete(
            '%s:registry:shards:name:%s'%(
                self._get_cluster_name(), meta_info['name'])
        )
        pipe.delete(
            '%s:registry:shards:uuid:%s'%(
                self._get_cluster_name(), shard_uuid)
        )
        try:
            pipe.execute()
            self._remove_empty_list(
                '%s:shards:%s'%(self._get_cluster_name(), meta_info['group']))
            return True
        except Exception as e:
            self.redice_errors.error_reg(
                'DeleteShard',
                'Delete shard transaction error: %s'%(e))

        return False

    def info_shard(self, obj, short=False):
        results_list = []
        shard_uuid = self._identify_uuid('shards', obj)
        info_shard = self.get_shards_info(shard_uuid)
        if info_shard:
            self._shards_print(
                {info_shard['group']: [info_shard]}, 'group', short)
            return True
        else:
            self.redice_errors.error_reg(
                'ShardInfo',
                'Shard info not assigned to %s'%(
                    obj))
            return False

    def _test(self):
        import time
        for i in range(100):
            self.create_map(
                'test_name%d'%(i), 1, 4)
            time.sleep(.01)
