package com.xingcloud.server.helper;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * User: IvyTang
 * Date: 12-12-3
 * Time: 上午11:49
 */
public class Dom {
    private Element e;

    private Dom(Element element) {
        this.e = element;
    }

    public Element getDomElement() {
        return e;
    }

    public static Dom getRoot(InputStream is) throws Exception {
        Document doc = null;
        try {
            DocumentBuilder dombuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            doc = dombuilder.parse(is);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Element root = doc.getDocumentElement();
        return new Dom(root);
    }

    public static Dom getRoot(String xmlFile) throws Exception {
        try {

            InputStream is = new FileInputStream(xmlFile);
            Dom root = getRoot(is);
            is.close();
            return root;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getAttributeValue(String attributeName) {
        return e.getAttribute(attributeName);
    }

    public boolean existElement(String elementName) {
        NodeList nodeList = e.getElementsByTagName(elementName);
        return !((nodeList == null) || (nodeList.getLength() < 1));
    }

    public String elementText(String elementName) {
        Element element = (Element) e.getElementsByTagName(elementName).item(0);
        Node textNode = element.getFirstChild();
        if (textNode == null) {
            return "";
        }
        return textNode.getNodeValue();
    }

    public String getSelfText() {
        Node textNode = e.getFirstChild();
        if (textNode == null) {
            return "";
        }
        return textNode.getNodeValue();
    }

    public Dom element(String elementName) {
        NodeList nodeList = e.getElementsByTagName(elementName);
        if ((nodeList == null) || (nodeList.getLength() < 1)) {
            return null;
        }
        Element element = (Element) nodeList.item(0);
        return new Dom(element);
    }

    public List<Dom> elements(String elementName) {
        List<Dom> eList = new ArrayList<Dom>();
        NodeList nodeList = e.getElementsByTagName(elementName);
        if (nodeList == null) {
            return eList;
        }
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element) node;
                eList.add(new Dom(element));
            }
        }
        return eList;
    }
}
