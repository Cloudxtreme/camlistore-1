# Copyright 2015 The Camlistore Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM sojournlabs/gcc
MAINTAINER Tamás Gulácsi <tgulacsi78+docker@gmail.com>

RUN gpg --keyserver pgp.mit.edu --recv-keys 0E44CF0C
RUN curl -o litmus-0.13.tar.gz 'http://www.webdav.org/neon/litmus/litmus-0.13.tar.gz' && \
	curl 'http://www.webdav.org/neon/litmus/litmus-0.13.tar.gz.asc' | \
		gpg --verify - litmus-0.13.tar.gz && \
	tar xzf litmus-0.13.tar.gz && \
	rm -f litmus-0.13.tar.gz* && \
	mv litmus-0.13 litmus
WORKDIR /vapr/litmus
RUN ./configure --prefix=$(pwd) && make -j5

CMD ["make", "URL=http://localhost:8080", "check"]
