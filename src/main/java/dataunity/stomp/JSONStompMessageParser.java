package dataunity.stomp;

import java.util.List;

import asia.stampy.common.message.AbstractBodyMessageHeader;
import asia.stampy.common.parsing.StompMessageParser;

// Note: duplicated in pipes worker. Potentially split out
// into separate library
public class JSONStompMessageParser extends StompMessageParser {
	protected boolean isText(List<String> headers) {
		// JSON can be treated like text for our purposes
		boolean isText = super.isText(headers);
		boolean isJSON = false;
		for (String hdr : headers) {
			if (hdr.contains(AbstractBodyMessageHeader.CONTENT_TYPE)) {
				if (hdr.contains("application/json")) {
					isJSON = true;
					break;
				}
			}
	    }
		return isText || isJSON;
	}
}
