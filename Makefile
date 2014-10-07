lint:
	@jshint --reporter node_modules/jshint-stylish/stylish.js lib/*.js

.PHONY: lint