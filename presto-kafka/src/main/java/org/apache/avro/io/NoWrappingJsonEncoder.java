/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;

import java.io.IOException;
import java.io.OutputStream;

public class NoWrappingJsonEncoder
        extends JsonEncoder
{
    public NoWrappingJsonEncoder(Schema sc, OutputStream out) throws IOException
    {
        super(sc, out);
    }

    public NoWrappingJsonEncoder(Schema sc, OutputStream out, boolean pretty) throws IOException
    {
        super(sc, out, pretty);
    }

    public NoWrappingJsonEncoder(Schema sc, JsonGenerator out) throws IOException
    {
        super(sc, out);
    }

    @Override
    public void writeIndex(int unionIndex) throws IOException
    {
        parser.advance(Symbol.UNION);
        Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
        Symbol symbol = top.getSymbol(unionIndex);
        parser.pushSymbol(symbol);
    }
}
