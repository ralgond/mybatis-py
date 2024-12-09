import xml.etree.ElementTree as et
from typing import Tuple, Set
import re

class MapperManager:
    def __init__(self):
        self.id_2_element_map = {}
        self.param_pattern = re.compile(r"#{([a-zA-Z0-9_\-]+)}")
        self.replace_pattern = re.compile(r"\${([a-zA-Z0-9_\-]+)}")

    def read_mapper_xml_file(self, mapper_xml_file_path):
        namespace = ""
        root = et.parse(mapper_xml_file_path).getroot()
        for child in root.iter():
            if child.tag == 'mapper':
                if "namespace" in child.attrib:
                    namespace = child.attrib["namespace"]
                root = child
                break

        for child in root:
            child_id = child.attrib["id"]
            if child_id is None:
                raise Exception("Missing id")
            if namespace == "":
                self.id_2_element_map[child_id] = child
            else:
                self.id_2_element_map[namespace+"."+child_id] = child


    # @staticmethod
    # def _trim_prefix(s : str, prefixes : Set[str]):
    #     l = [term.strip() for term in s.split()]
    #     idx = 0
    #     for term in l:
    #         if term in prefixes:
    #             idx += 1
    #             continue
    #         else:
    #             break
    #
    #     return " ".join(l[idx:])

    @staticmethod
    def _trim_prefix(s: str, prefixes: Set[str]):
        p = [term[0] for term in sorted([(prefix, len(prefix)) for prefix in prefixes], key=lambda x: x[1], reverse=True)]
        l = [term.strip() for term in s.split()]
        s = " ".join(l)
        while True:
            to_break = True
            for prefix in p:
                if s.startswith(prefix):
                    s = s[len(prefix):].strip()
                    to_break = False
                    break
            if to_break:
                break

        return s.strip()

    @staticmethod
    def _trim_suffix(s: str, suffixes: Set[str]):
        suffixes = [term[0] for term in
             sorted([(suffix, len(suffix)) for suffix in suffixes], key=lambda x: x[1], reverse=True)]
        l = [term.strip() for term in s.split()]
        s = " ".join(l)
        while True:
            to_break = True
            for suffix in suffixes:
                if s.endswith(suffix):
                    s = s[:-1*len(suffix)].strip()
                    to_break = False
                    break
            if to_break:
                break

        return s.strip()

    def parse_element(self, element : et.Element, param: dict) -> str:
        # print ("++++>", element)
        ret = ""
        # if element.text is not None:
        #     ret += element.text

        if element.tag == "include":
            refid = element.attrib['refid']
            ref_element = self.id_2_element_map[refid]
            ret = ""
            ret += self.parse_element(ref_element, param)
            return ret
        elif element.tag == "sql":
            ret += element.text
            for child in element:
                ret += self.parse_element(child, param)
                ret += child.tail
            return ret
        elif element.tag == "if":
            test_attribute = element.attrib['test']
            ok = eval(test_attribute, param)
            if ok:
                ret += element.text
                for child in element:
                    ret += self.parse_element(child, param)
                    ret += child.tail
            return ret
        elif element.tag == "where":
            temp = ""
            if element.text is not None:
                temp += element.text
            for child in element:
                temp += self.parse_element(child, param)
                if child.tail is not None:
                    temp += child.tail

            temp = temp.strip()
            l = temp.split()
            idx = 0
            for _ in range(0, len(l)):
                if l[idx].upper() == 'AND' or l[idx].upper() == 'OR':
                    idx += 1
                    continue
                else:
                    break

            if len(l) == idx:
                return ""
            else:
                return "WHERE " + ' '.join(l[idx:])
        elif element.tag == "trim":
            prefix = ""
            if 'prefix' in element.attrib:
                prefix = element.attrib['prefix']

            suffix = ""
            if 'suffix' in element.attrib:
                suffix = element.attrib['suffix']

            if 'prefixOverrides' in element.attrib:
                prefix_overrides_set = set([term.strip() for term in element.attrib['prefixOverrides'].split("|")])
            else:
                prefix_overrides_set = set()

            if 'suffixOverrides' in element.attrib:
                suffix_overrides_set = set([term.strip() for term in element.attrib['suffixOverrides'].split("|")])
            else:
                suffix_overrides_set = set()

            ret = ""
            if element.text is not None:
                ret += element.text
            for child in element:
                ret += self.parse_element(child, param)
                ret += child.tail

            if len(prefix_overrides_set) > 0:
                ret = MapperManager._trim_prefix(ret, prefix_overrides_set)

            if len(suffix_overrides_set) > 0:
                ret = MapperManager._trim_suffix(ret, suffix_overrides_set)

            return prefix + " " + ret + " " + suffix

        elif element.tag == "set":
            prefix_overrides_set = set()
            prefix_overrides_set.add(",")

            ret = ""
            if element.text is not None:
                ret += element.text
            for child in element:
                ret += self.parse_element(child, param)
                ret += child.tail

            ret = MapperManager._trim_prefix(ret, prefix_overrides_set)
            return "SET " + ret

        elif element.tag == "choose":
            ok = False
            for child in element:
                if child.tag == "when":
                    test_condition = child.attrib['test']
                    ok = eval(test_condition, param)
                    if ok:
                        ret += self.parse_element(child, param)
                        break

            if ok:
                return ret

            otherwise_ele = None
            for child in element:
                if child.tag == "otherwise":
                    otherwise_ele = child
                    break

            if otherwise_ele is None:
                raise Exception("Missing otherwise element")

            ret = self.parse_element(otherwise_ele, param)
            return ret
        elif element.tag == "when":
            ret = ""
            if element.text is not None:
                ret += element.text
            for child in element:
                ret += self.parse_element(child, param)
                ret += child.tail
            return ret
        elif element.tag == "otherwise":
            ret = ""
            if element.text is not None:
                ret += element.text
            for child in element:
                ret += self.parse_element(child, param)
                ret += child.tail
            return ret
        elif element.tag == "foreach":
            item = element.attrib['item']
            collection = element.attrib['collection']
            open = element.attrib['open']
            close = element.attrib['close']
            separator = element.attrib['separator']

            islist = eval("isinstance("+collection+",list)", param['params'])
            if not islist:
                raise Exception("Collection must be a list")

            eval_string = f"[item for item in {collection}]"
            l = eval(eval_string, param['params'])

            child_ret_l = []
            for index, _ in enumerate(l):
                child_ret = ""
                if element.text is not None:
                    child_ret += element.text
                for child in element:
                    child_ret += self.parse_element(child, param)
                    child_ret += child.tail

                old_string = "#{" + item + "}"
                new_string = "#{" + collection + "-" + str(index) + "}"
                child_ret = child_ret.replace(old_string, new_string)
                child_ret_l.append(child_ret)

            ret = open + separator.join(child_ret_l) + close

            return ret

    @staticmethod
    def _format_sql(sql):
        sql = sql.strip()
        l = sql.split()
        return ' '.join(l)

    def _to_prepared_statement(self, ret, param) -> Tuple[str, list]:
        ret_param = []
        matches = self.param_pattern.findall(ret)
        for match in matches:
            if '-' in match:
                container_name, index = match.split('-', 1)
                # print("====>", param[container_name][int(index)])
                ret_param.append(param[container_name][int(index)])
            else:
                if match in param:
                    ret_param.append(param[match])
                else:
                    ret_param.append(None)

        ret = self.param_pattern.sub("?", ret)

        ret = MapperManager._format_sql(ret)

        return (ret, ret_param)

    def _to_replace(self, ret, param) -> str:
        matches = self.replace_pattern.findall(ret)
        for match in matches:
            value = ""
            if match in param:
                value = param[match]
            ret = ret.replace("${"+match+"}", value)
        return ret


    def select(self, id: str, params: dict) -> Tuple[str, list]:
        if id not in self.id_2_element_map:
            raise Exception("Missing id")
        element = self.id_2_element_map[id]
        if element.tag != "select":
            raise Exception("Not a select")

        ret = ""
        if element.text is not None:
            ret += element.text
        param0 = {'params': params}
        for child in element:
            ret += self.parse_element(child, param0)
            ret += child.tail

        sql, sql_param = self._to_prepared_statement(ret, params)
        sql = self._to_replace(sql, params)
        return (sql, sql_param)

    def update(self, id: str, params: dict) -> Tuple[str, list]:
        if id not in self.id_2_element_map:
            raise Exception("Missing id")
        element = self.id_2_element_map[id]
        if element.tag != "update":
            raise Exception("Not an update")

        ret = ""
        if element.text is not None:
            ret += element.text
        param0 = {'params': params}
        for child in element:
            ret += self.parse_element(child, param0)
            ret += child.tail

        sql, sql_param = self._to_prepared_statement(ret, params)
        sql = self._to_replace(sql, params)
        return (sql, sql_param)

    def delete(self, id: str, params: dict) -> Tuple[str, list]:
        if id not in self.id_2_element_map:
            raise Exception("Missing id")
        element = self.id_2_element_map[id]
        if element.tag != "delete":
            raise Exception("Not a delete")

        ret = ""
        if element.text is not None:
            ret += element.text
        param0 = {'params': params}
        for child in element:
            ret += self.parse_element(child, param0)
            ret += child.tail

        sql, sql_param = self._to_prepared_statement(ret, params)
        sql = self._to_replace(sql, params)
        return (sql, sql_param)

    def insert(self, id: str, params: dict, primary_key:str=None) -> Tuple[str, list]:
        if id not in self.id_2_element_map:
            raise Exception("Missing id")
        element = self.id_2_element_map[id]
        if element.tag != "insert":
            raise Exception("Not an insert")

        ret = ""
        if element.text is not None:
            ret += element.text
        param0 = {'params': params}
        for child in element:
            ret += self.parse_element(child, param0)
            ret += child.tail

        sql, sql_param = self._to_prepared_statement(ret, params)
        sql = self._to_replace(sql, params)

        if primary_key:
            sql += (" RETURNING "+str(primary_key))

        return (sql, sql_param)