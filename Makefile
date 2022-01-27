# Requirements for xcode project generation:
# sudo easy_install pip
# sudo pip install pbxproj

SWIFT_BUILD_FLAGS=--configuration release

all: fix_module_header_apple build
	
fix_module_header_apple:
	# on M1 Macs, the path to libpq-fe.h header is /opt/homebrew/include/libpq-fe.h
	# on any sane Mac, the path to libpq-fe.h header is /usr/local/include/libpq-fe.h
	if [ -f /opt/homebrew/include/libpq-fe.h ] ; then echo 'module libpq [system] {\n\theader "/opt/homebrew/include/libpq-fe.h"\n\tlink "libpq"\n\texport *\n}' > Sources/libpq-apple/module.modulemap; fi;
	if [ -f /usr/local/include/libpq-fe.h ] ; then echo 'module libpq [system] {\n\theader "/usr/local/include/libpq-fe.h"\n\tlink "libpq"\n\texport *\n}' > Sources/libpq-apple/module.modulemap; fi;

	#TODO: also change the path in the Makefile for M1 macs

build:
	./meta/CombinedBuildPhases.sh
	swift build -v $(SWIFT_BUILD_FLAGS)

clean:
	rm -rf .build

test:
	swift test -v

update:
	swift package update

xcode:
	swift package generate-xcodeproj
	meta/addBuildPhase rover.xcodeproj/project.pbxproj "Rover::RoverFramework" 'export PATH=/opt/homebrew/bin:/opt/homebrew/sbin:$PATH; cd $${SRCROOT}; ./meta/CombinedBuildPhases.sh'
	sleep 2
	open rover.xcodeproj
