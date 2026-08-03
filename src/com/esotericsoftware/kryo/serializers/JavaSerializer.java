/* Copyright (c) 2008-2025, Nathan Sweet
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 * 
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided with the distribution.
 * - Neither the name of Esoteric Software nor the names of its contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. */

package com.esotericsoftware.kryo.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.ObjectMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.function.Predicate;

/** Serializes objects using Java's built in serialization mechanism. Note that this is very inefficient and should be avoided if
 * possible.
 * @see Serializer
 * @see FieldSerializer
 * @see KryoSerializable
 * @author Nathan Sweet */
public class JavaSerializer extends Serializer {
	private Predicate<Class> classFilter;

	/** Sets an optional filter applied to each class encountered while deserializing. When set, a class for which the predicate
	 * returns false is rejected with a {@link KryoException} before it is used, providing opt-in, defense-in-depth protection when
	 * reading serialized data from an untrusted source. When null (the default) no filtering is applied and behavior is unchanged.
	 * @param classFilter May be null. */
	public void setClassFilter (Predicate<Class> classFilter) {
		this.classFilter = classFilter;
	}

	/** @return May be null. */
	public Predicate<Class> getClassFilter () {
		return classFilter;
	}

	public void write (Kryo kryo, Output output, Object object) {
		try {
			ObjectMap graphContext = kryo.getGraphContext();
			ObjectOutputStream objectStream = (ObjectOutputStream)graphContext.get(this);
			if (objectStream == null) {
				objectStream = new ObjectOutputStream(output);
				graphContext.put(this, objectStream);
			}
			objectStream.writeObject(object);
			objectStream.flush();
		} catch (Exception ex) {
			throw new KryoException("Error during Java serialization.", ex);
		}
	}

	public Object read (Kryo kryo, Input input, Class type) {
		try {
			ObjectMap graphContext = kryo.getGraphContext();
			ObjectInputStream objectStream = (ObjectInputStream)graphContext.get(this);
			if (objectStream == null) {
				objectStream = new ObjectInputStreamWithKryoClassLoader(input, kryo, classFilter);
				graphContext.put(this, objectStream);
			}
			return objectStream.readObject();
		} catch (Exception ex) {
			throw new KryoException("Error during Java deserialization.", ex);
		}
	}

	/** {@link ObjectInputStream} uses the last user-defined {@link ClassLoader}, which may not be the correct one. This is a known
	 * Java issue and is often solved by using a specific class loader. See:
	 * https://github.com/apache/spark/blob/v1.6.3/streaming/src/main/scala/org/apache/spark/streaming/Checkpoint.scala#L154
	 * https://issues.apache.org/jira/browse/GROOVY-1627 */
	private static class ObjectInputStreamWithKryoClassLoader extends ObjectInputStream {
		private final Kryo kryo;
		private final Predicate<Class> classFilter;

		ObjectInputStreamWithKryoClassLoader (InputStream in, Kryo kryo, Predicate<Class> classFilter) throws IOException {
			super(in);
			this.kryo = kryo;
			this.classFilter = classFilter;
		}

		protected Class resolveClass (ObjectStreamClass type) {
			Class resolved;
			try {
				resolved = Class.forName(type.getName(), false, kryo.getClassLoader());
			} catch (ClassNotFoundException ignored) {
				try {
					resolved = super.resolveClass(type);
				} catch (ClassNotFoundException ex) {
					throw new KryoException("Class not found: " + type.getName(), ex);
				} catch (IOException ex) {
					throw new KryoException("Could not load class: " + type.getName(), ex);
				}
			}
			if (classFilter != null && !classFilter.test(resolved)) {
				throw new KryoException("Deserialization is not allowed for class: " + type.getName());
			}
			return resolved;
		}
	}
}
