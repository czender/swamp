# $Id$ 
# Makefile for swamp demo applications
#
# Copyright 2007 Daniel L. Wang and Charles S. Zender
# This file is part of the SWAMP project and is released under
# the terms of the GNU General Public License version 3 (GPLv3)
#
#

CLIENTTAR = swamp-client.tar.bz2
SOURCETAR = swamp.tar.bz2
SERVICETAR = swamp-service.tar.bz2
TAGSERVICETAR = swamp-service-$(TAG).tar.bz2
DISTDIR = dist
CLIENT_DISTDIR = $(DISTDIR)/swamp-client
SOURCE_DISTDIR = $(DISTDIR)/swamp
SERVICE_DISTDIR = $(DISTDIR)/swamp-service

CLIENTTARPATH = $(DISTDIR)/$(CLIENTTAR)
SOURCETARPATH = $(DISTDIR)/$(SOURCETAR)
SERVICETARPATH = $(DISTDIR)/$(SERVICETAR)
TAGSERVICEPATH = $(DISTDIR)/$(TAGSERVICETAR)

TARBALLS = $(CLIENTTARPATH) $(SOURCETARPATH) $(SERVICETARPATH)


# build the distribution tarballs
clientdist:	$(CLIENTTARPATH)

sourcedist:	$(SOURCETARPATH)

servicedist:	$(SERVICETARPATH)

tagserver:	$(TAGSERVICEPATH)

clean:	
	rm -rf $(TARBALLS) $(DISTDIR)
	rm src/*{~,pyc} src/swamp/*{pyc,~}

CLIENTFILES = src/swamp_client.py src/swamp_transact.py demo/mtosca.swamp \
 demo/ipcctest.swamp doc/uci/* doc/HOWTO-Client.txt 



$(CLIENTTARPATH):	$(CLIENTFILES)
	mkdir -p $(CLIENT_DISTDIR)
	cp $(CLIENTFILES) $(CLIENT_DISTDIR)
	cd $(DISTDIR) && tar cjvf $(CLIENTTAR) swamp-client
	rm -r $(CLIENT_DISTDIR)
	md5sum $(CLIENTTARPATH) > $(CLIENTTARPATH).md5


$(SOURCETARPATH):	# everything under svn
	mkdir -p $(DISTDIR)
	svn export http://swamp.googlecode.com/svn/trunk/ $(SOURCE_DISTDIR)
	cd $(DISTDIR) && tar cjvf $(SOURCETAR) swamp
	rm -r $(SOURCE_DISTDIR)
	md5sum $(SOURCETARPATH) > $(SOURCETARPATH).md5

SERVICEFILES_ = doc/HOWTO-Service.txt demo/sample-swamp.conf src/{swamp_soapinterface.py,swamp_transact.py,workerServer.py,swamp_common.py}

$(SERVICETARPATH):	$(SERVICEFILES) src/swamp/* # everything the server needs
	mkdir -p $(SERVICE_DISTDIR)
	cp -r $(SERVICEFILES_) $(SERVICE_DISTDIR)
	svn export http://swamp.googlecode.com/svn/trunk/src/swamp $(SERVICE_DISTDIR)/swamp
	cd $(DISTDIR) && tar cjvf $(SERVICETAR) swamp-service
	rm -r $(SERVICE_DISTDIR)
	md5sum $(SERVICETARPATH) > $(SERVICETARPATH).md5

# this is a little broken: we pull swamp/ from repo, but everything else from the working dir.
$(TAGSERVICEPATH):	 
	echo "building tag --$(TAG)--"
	mkdir -p $(SERVICE_DISTDIR)
	svn export http://swamp.googlecode.com/svn/tags/$(TAG) $(SERVICE_DISTDIR)/tmp_buildtree
	cp -r $(addprefix $(SERVICE_DISTDIR)/tmp_buildtree/,$(SERVICEFILES_)) $(SERVICE_DISTDIR)
	cp -r $(SERVICE_DISTDIR)/tmp_buildtree/src/swamp $(SERVICE_DISTDIR)/swamp
	rm -r $(SERVICE_DISTDIR)/tmp_buildtree
	cd $(DISTDIR) && tar cjvf $(TAGSERVICETAR) swamp-service
	rm -r $(SERVICE_DISTDIR)
	md5sum $(TAGSERVICEPATH) > $(TAGSERVICEPATH).md5

