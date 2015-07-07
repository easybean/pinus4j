/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.pinus4j.constant.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * 解析xml的辅助工具.
 * 
 * @author duanbn
 */
public class XmlUtil {

    public static final Logger     LOG     = LoggerFactory.getLogger(XmlUtil.class);

    private DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    private Document               xmlDoc;

    /**
     * 构造方法. 读取classpath根路径下的xml文件.
     * 
     * @param xmlFileName classpath根路径下的xml文件名
     */
    private XmlUtil() {
        InputStream is = null;
        String xmlFileName = Const.DEFAULT_CONFIG_FILENAME;
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(xmlFileName);
            xmlDoc = builder.parse(new InputSource(is));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("读取classpath根路径的xml失败, file name " + xmlFileName);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    /**
     * 构造方法. 读取指定的xml文件
     * 
     * @param xmlFile 指定的xml文件
     */
    private XmlUtil(File xmlFile) {
        InputStream is = null;
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            is = new FileInputStream(xmlFile);
            xmlDoc = builder.parse(new InputSource(is));
        } catch (Exception e) {
            throw new RuntimeException("读取classpath根路径的xml失败, file " + xmlFile.getAbsolutePath());
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    public static XmlUtil getInstance() {
        return new XmlUtil();
    }

    public static XmlUtil getInstance(File xmlFile) {
        return new XmlUtil(xmlFile);
    }

    public Node getRoot() {
        NodeList childs = xmlDoc.getChildNodes();
        if (childs == null || childs.getLength() == 0) {
            return null;
        }
        return xmlDoc.getChildNodes().item(0);
    }

    public Node getFirstChildByName(Node parent, String name) {
        NodeList childs = parent.getChildNodes();
        if (childs == null || childs.getLength() == 0) {
            return null;
        }

        Node node = null;
        for (int i = 0; i < childs.getLength(); i++) {
            node = childs.item(i);
            if (node.getNodeName().equals(name)) {
                return node;
            }
        }

        return null;
    }

    public List<Node> getChildByName(Node parent, String name) {
        List<Node> list = new ArrayList<Node>();
        NodeList childs = parent.getChildNodes();
        if (childs == null || childs.getLength() == 0) {
            return null;
        }

        Node node = null;
        for (int i = 0; i < childs.getLength(); i++) {
            node = childs.item(i);
            if (node.getNodeName().equals(name)) {
                list.add(node);
            }
        }

        return list;
    }

    public Map<String, String> getAttributeAsMap(Node node, String... ignores) {
        Map<String, String> propMap = new HashMap<String, String>();
        if (node == null) {
            return propMap;
        }

        NamedNodeMap attrMap = node.getAttributes();
        Node attr = null;
        for (int i = 0; i < attrMap.getLength(); i++) {
            attr = attrMap.item(i);
            propMap.put(attr.getNodeName(), attr.getNodeValue());
        }
        for (String ignore : ignores) {
            propMap.remove(ignore);
        }

        return propMap;
    }

    public String getAttributeValue(Node node, String attribute) {
        if (node == null) {
            return null;
        }

        NamedNodeMap namedNodeMap = node.getAttributes();
        if (namedNodeMap == null) {
            return null;
        }

        Node attrNode = namedNodeMap.getNamedItem(attribute);
        if (attrNode == null) {
            return null;
        }

        return attrNode.getNodeValue();
    }

}
