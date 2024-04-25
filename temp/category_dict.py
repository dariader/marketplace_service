from xml.etree.ElementTree import iterparse

category_mapping = {}
category_annotation = {}
for _, elem in iterparse(source="temp_data.xml", events=("end",)):
    offer_elem = elem
    if elem.tag == "category":
        id = int(elem.attrib.get("id"))
        value = int(elem.attrib.get("parentId", 0))
        descr = elem.text
        category_mapping[id] = value
        category_annotation[id] = descr


def get_parents(id, lst=[]):
    if id != 90401:
        parent = category_mapping.get(id)
        get_parents(parent, lst + [category_annotation.get(parent)])
    else:
        print(lst)


get_parents(17677674, lst=[])
