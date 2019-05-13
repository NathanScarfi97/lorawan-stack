// Copyright © 2019 The Things Network Foundation, The Things Industries B.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import React from 'react'
import { Container, Row, Col } from 'react-grid-system'
import { defineMessages } from 'react-intl'

import Button from '../../../components/button'
import Icon from '../../../components/icon'
import Message from '../../../lib/components/message'
import ErrorMessage from '../../../lib/components/error-message'
import PropTypes from '../../../lib/prop-types'

import { isBackend, isNotFoundError } from '../../../lib/errors/utils'

import style from './sub-view.styl'

const m = defineMessages({
  errorTitle: 'We\'re sorry!',
  errorExplanation: 'There was a problem when displaying this section.',
  contactAdministrator: 'If the error persists after refreshing, please contact an administrator.',
  additionalInformation: 'Additional Information (please attach to error inquiries)',
  refresh: 'Refresh page',
})

const reload = () => location.reload()

const SubViewError = function ({ error }) {
  return (
    <Container>
      <Row>
        <Col>
          <div className={style.title}>
            <Icon icon="error_outline" large />
            <Message component="h2" content={m.errorTitle} />
          </div>
          <p>
            <Message component="span" content={m.errorExplanation} /><br />
            <Message component="span" content={m.contactAdministrator} />
          </p>
          { isBackend(error) && (
            <React.Fragment>
              <hr />
              <ErrorMessage content={error} />
            </React.Fragment>
          ) }
          { !isNotFoundError(error) && (
            <Button
              className={style.button}
              icon="refresh"
              message={m.refresh}
              onClick={reload}
            />
          )}
        </Col>
      </Row>
    </Container>
  )
}

SubViewError.propTypes = {
  error: PropTypes.error.isRequired,
}

export default SubViewError
