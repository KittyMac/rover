# Requirements for xcode project generation:
# sudo easy_install pip
# sudo pip install pbxproj

SWIFT_BUILD_FLAGS=--configuration release

all: build

brew:
	brew install libpq

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
	meta/addBuildPhase rover.xcodeproj/project.pbxproj "Rover::Rover" 'export PATH=/opt/homebrew/bin:/opt/homebrew/sbin:$PATH; cd $${SRCROOT}; ./meta/CombinedBuildPhases.sh'
	sleep 2
	open rover.xcodeproj
