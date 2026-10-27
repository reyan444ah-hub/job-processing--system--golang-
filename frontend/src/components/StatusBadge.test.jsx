import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import StatusBadge from './StatusBadge'

describe('StatusBadge', () => {
  it('labels supported backend job statuses', () => {
    render(<StatusBadge status="dead_letter" />)
    expect(screen.getByText('Dead letter')).toBeInTheDocument()
  })

  it('renders unknown statuses safely', () => {
    render(<StatusBadge status="future_state" />)
    expect(screen.getByText('future_state')).toBeInTheDocument()
  })
})
