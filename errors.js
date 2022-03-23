// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

class ErrorWithCode extends Error {
  constructor(message, code) {
    super(message)
    this.code = code
  }
}

function nanOffsetErr(offset) {
  return new ErrorWithCode(
    `Offset ${offset} is not a number`,
    'ERR_AAOL_INVALID_OFFSET'
  )
}

function negativeOffsetErr(offset) {
  return new ErrorWithCode(
    `Offset ${offset} is negative`,
    'ERR_AAOL_INVALID_OFFSET'
  )
}

function outOfBoundsOffsetErr(offset, logSize) {
  return new ErrorWithCode(
    `Offset ${offset} is beyond log size ${logSize}`,
    'ERR_AAOL_OFFSET_OUT_OF_BOUNDS'
  )
}

function deletedRecordErr() {
  return new ErrorWithCode('Record has been deleted', 'ERR_AAOL_DELETED_RECORD')
}

module.exports = {
  ErrorWithCode,
  nanOffsetErr,
  negativeOffsetErr,
  outOfBoundsOffsetErr,
  deletedRecordErr,
}
