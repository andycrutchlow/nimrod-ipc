/*
 * Copyright 2014 Andrew Crutchlow
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nimrodtechs.exceptions;

public class NimrodRmiTransportException extends NimrodRmiException {

	public NimrodRmiTransportException() {
		super();
		//
	}

	public NimrodRmiTransportException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		//
	}

	public NimrodRmiTransportException(String message) {
		super(message);
		//
	}

	public NimrodRmiTransportException(Throwable cause) {
		super(cause);
		//
	}

	public NimrodRmiTransportException(String message, Throwable cause) {
		super(message, cause);
	}

}
