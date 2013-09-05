import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.w3c.dom.Node;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;

import amos.note.Note;

/**
 *	XML�ļ�У�鼰��java�����໥ӳ��Ĺ���
 */
public class XMLUtil {
//	private static Logger logger = LoggerFactory.getLogger(XMLUtil.class);//slf4j logging
	
	/**
	 * ����ָ���������İ�·������ʵ��
	 * @param contextPath
	 * @throws JAXBException
	 */
	public XMLUtil(String contextPath) throws JAXBException{
		this.resetContext(contextPath);
	}
	
	/**
	 * ����ָ���������İ�·������������������
	 * @param contextPath
	 * @throws JAXBException
	 */
	public void resetContext(String contextPath) throws JAXBException{
		this.jaxbContext = JAXBContext.newInstance(contextPath);
		this.unmarshaller = this.jaxbContext.createUnmarshaller();
		this.marshaller = this.jaxbContext.createMarshaller();
	}
	
	/**
	 * ���xsd�ļ�У��һ��xml�ļ��Ƿ���Ч
	 * @param xmlFilePath
	 * @param xsdFilePath
	 * @return true-��Ч false-��Ч
	 * @throws IOException
	 * @throws SAXException
	 */
	public static boolean validate(String xmlFilePath, String xsdFilePath) throws IOException, SAXException{
		return XMLUtil.doValidate(xmlFilePath, xsdFilePath, null);
	}

	
	/**
	 * ���xsd�ļ�У��һ��xml�ļ��Ƿ���Ч������Ч�쳣ʱ����errorHandler�����쳣
	 * @param xmlFilePath
	 * @param xsdFilePath
	 * @param errorHandler
	 * @throws IOException
	 * @throws SAXException
	 */
	public static void validate(String xmlFilePath, String xsdFilePath, ErrorHandler errorHandler) throws IOException, SAXException{
		XMLUtil.doValidate(xmlFilePath, xsdFilePath, errorHandler);
	}
	
	/**
	 * ���xsd�ļ�У��һ��xml�ļ��Ƿ���Ч������Ч�쳣ʱ����errorHandler�����쳣
	 * 
	 * @param xmlFilePath
	 * @param xsdFilePath
	 * @param errorHandler
	 * @return
	 * @throws IOException
	 * @throws SAXException
	 */
	private static boolean doValidate(String xmlFilePath, String xsdFilePath,
			ErrorHandler errorHandler) throws IOException, SAXException {
		boolean rt = false;
		// 1. Lookup a factory for the W3C XML Schema language
//		SchemaFactory factory = SchemaFactory
//				.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		
		SchemaFactory factory = SchemaFactory
				.newInstance("http://www.w3.org/2001/XMLSchema");
		// 2. Compile the schema.
		// Here the schema is loaded from a java.io.File, but you could use
		// a java.net.URL or a javax.xml.transform.Source instead.
		File schemaLocation = new File(xsdFilePath);
		Schema schema = factory.newSchema(schemaLocation);

		// 3. Get a validator from the schema.
		Validator validator = schema.newValidator();
		validator.setErrorHandler(errorHandler);

		// 4. Parse the document you want to check.
		Source source = new StreamSource(xmlFilePath);

		// 5. Check the document
		try {
			validator.validate(source);
			rt = true;
		} catch (SAXException ex) {
			rt = false;
		}
		return rt;
	}
	
	/**
	 * ��java����ӳ�䵽xml�ļ���
	 * @param jaxbElement
	 * @param outputXMLFile
	 * @throws FileNotFoundException
	 * @throws JAXBException
	 */
	public void marshal(Object jaxbElement, String outputXMLFile)
			throws FileNotFoundException, JAXBException {
		this.marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
				new Boolean(true));
		this.marshaller.marshal(jaxbElement,
				new FileOutputStream(outputXMLFile));
	}
	
	/**
	 * ��java����ӳ��Ϊ�����
	 * @param jaxbElement
	 * @param os �����������
	 * @throws FileNotFoundException
	 * @throws JAXBException
	 */
	public void marshal(Object jaxbElement, OutputStream os)
			throws FileNotFoundException, JAXBException {
		this.marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
				new Boolean(true));
		this.marshaller.marshal(jaxbElement,os);
	}
	
	/**
	 * ��java����ӳ��ΪDOM node
	 * @param jaxbElement
	 * @param node_output �����������
	 * @throws FileNotFoundException
	 * @throws JAXBException
	 */
	public void marshal(Object jaxbElement, Node node_output)
			throws FileNotFoundException, JAXBException {
		this.marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
				new Boolean(true));
		this.marshaller.marshal(jaxbElement,node_output);
	}
	
	/**
	 * ��xml�ļ�ӳ��Ϊjava����
	 * @param inputXMLFile
	 * @return
	 * @throws JAXBException
	 */
	public Object unmarshal(String inputXMLFile) throws JAXBException{
		return this.unmarshaller.unmarshal(new File(inputXMLFile));
	}
	
	/**
	 * ��DOM nodeӳ��Ϊjava����
	 * @param node
	 * @return
	 * @throws JAXBException
	 */
	public Object unmarshal(Node node) throws JAXBException{
		return this.unmarshaller.unmarshal(node);
	}
	
	/**
	 * ��������ӳ��Ϊjava����
	 * @param is
	 * @return
	 * @throws JAXBException
	 */
	public Object unmarshal(InputStream is) throws JAXBException{
		return this.unmarshaller.unmarshal(is);
	}
	
	//jaxb�õ��ı���
	private JAXBContext jaxbContext;
	private Unmarshaller unmarshaller;
	private Marshaller marshaller;
	
	/**
	 * @param args
	 * @throws JAXBException 
	 * @throws SAXException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws JAXBException, IOException, SAXException {
		XMLUtil util = new XMLUtil("amos.note");
		 //��֤
//		boolean flag = util.validate("src/main/java/t.xml", "src/main/java/t.xsd");
//		System.out.println(flag);
		//ӳ��Ϊjava����
		Object obj = util.unmarshal("src/main/java/t1.xml");

		//����
//		Note note = (Note) obj;
		
//		Note note = new Note();
//		note.setBody("hello,this is new xml file");
		//ӳ��Ϊxml�ļ�
//		util.marshal(note, "src/main/java/t1.xml");
		
	}
}
