
package com.kunyan;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the zxc package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _Sendmail_QNAME = new QName("http://zxc/", "sendmail");
    private final static QName _SendmailResponse_QNAME = new QName("http://zxc/", "sendmailResponse");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: zxc
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link SendMailResponse }
     * 
     */
    public SendMailResponse createSendmailResponse() {
        return new SendMailResponse();
    }

    /**
     * Create an instance of {@link SendMail }
     * 
     */
    public SendMail createSendmail() {
        return new SendMail();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SendMail }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://zxc/", name = "sendmail")
    public JAXBElement<SendMail> createSendmail(SendMail value) {
        return new JAXBElement<SendMail>(_Sendmail_QNAME, SendMail.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SendMailResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://zxc/", name = "sendmailResponse")
    public JAXBElement<SendMailResponse> createSendmailResponse(SendMailResponse value) {
        return new JAXBElement<SendMailResponse>(_SendmailResponse_QNAME, SendMailResponse.class, null, value);
    }

}
