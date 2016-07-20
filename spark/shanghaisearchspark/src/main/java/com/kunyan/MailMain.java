package com.kunyan;

public class MailMain {

	/**
 * 	 * @param args
 * 	 	 */
	public static void sendMail(String[] args) {
		
	    if (args.length < 3) {
	        System.out.println("Usage: [topic] [content] [mail]");
		}

		try {
			MailUtilWebService service = new MailUtilWebService();
			MailUtilWeb helloProxy = service.getMailUtilWebPort();

			helloProxy.sendmail(args[0], args[1], args[2]);
			System.out.println("send success !!");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	}

 

