/*
* Copyright 2013 Aven Solutions Pty Ltd
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

package com.avensolutions.baconbits;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.util.WrappedIOException;

@SuppressWarnings({ "deprecation" })
public class TuplesToString extends EvalFunc<String>
{
	@Override
    public String exec(Tuple input) throws IOException {
        String finalStr = "";
        Object values = input.get(0);
        if (values instanceof DataBag)
            try{
                for (Tuple tuple : (DataBag)values)
                {
                	String str = (String)tuple.toString().replaceAll("\\(|\\)", "").concat(" ");
                	finalStr = finalStr.concat(str);
                }
                return finalStr;
            }catch(Exception e){
                throw WrappedIOException.wrap("Caught exception processing input row ", e);
            }
        else
        	return null;
        	
     }

}