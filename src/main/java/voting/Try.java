package voting;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Try {
	static final int standardColumnsNumber = 8;
	public static void main(String[] args) {
		//System.out.printf("\n");
		Map<String,String> path = new HashMap<String,String>();
		path.put("1", "11");
		path.put("1", "111");//Ç°±ßµÄ11±»¸²¸Ç
		path.put("2", "22");
		for(String s : path.keySet()) {
			System.out.printf("%s %s\n",s,path.get(s));
		}
	}
}
