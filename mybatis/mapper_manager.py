import xml.etree.ElementTree as ET
from typing import Tuple
import re

class MapperManager:
    def __init__(self):
        self.id_2_element_map = {}
        self.param_pattern = re.compile(r"#{([a-zA-Z0-9_\-]+)}")
        #self.param_pattern_collection = re.compile(r"#{([a-zA-Z0-9_\-]+)}")

    def read_mapper_xml_file(self, mapper_xml_file_path):
        root = ET.parse(mapper_xml_file_path).getroot()
        for child in root.iter():
            if child.tag == 'mapper':
                root = child
                break

        for child in root:
            child_id = child.attrib["id"]
            if child_id is None:
                raise Exception("Missing id")
            self.id_2_element_map[child_id] = child


    def debug_dump(self):
        for child_id, child in self.id_2_element_map.items():
            print(child_id)
            print(child.text)

    def parse_element(self, element : ET.Element, param: dict) -> str:
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
            ret = ""
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


    def _format_sql(self, sql):
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

        ret = self._format_sql(ret)

        return (ret, ret_param)

    def select(self, id: str, param: dict) -> Tuple[str, list]:
        if id not in self.id_2_element_map:
            raise Exception("Missing id")
        element = self.id_2_element_map[id]
        if element.tag != "select":
            raise Exception("Not a select")

        ret = ""
        if element.text is not None:
            ret += element.text
        param0 = {'params': param}
        for child in element:
            ret += self.parse_element(child, param0)
            ret += child.tail

        return self._to_prepared_statement(ret, param)