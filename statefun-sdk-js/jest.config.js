export default {
    "roots": [
      "<rootDir>"
    ],
    "testMatch": [
      "**/test/**/*.+(ts|tsx|js)",
      "**/?(*.)+(spec|test).+(ts|tsx|js)"
    ],
    "transform": {
      "^.+\\.(ts|tsx)$": "ts-jest"
    },
    "preset": "ts-jest",
    "testEnvironment": 'node'
}
