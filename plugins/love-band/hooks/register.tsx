import type { Register } from 'claude-code'

export const register: Register = on => {
  on('session.start', ($, e, next) => {
    $.ui.toast('💖 я люблю тебе 💖')

    return next(e)
  })

  on('ui.render', { component: 'AbovePrompt' }, ($, e, next) => {
    if (e.props.hasSurvey) {
      return next(e)
    }

    const { Box, Text } = $.ui.resolve(e)

    return (
      <Box
        key="love"
        borderStyle="round"
        borderColor="#ff5fa2"
        paddingX={2}
        justifyContent="center"
        hover={{ borderColor: '#ff1f7a' }}
      >
        <Text bold color="#ff5fa2" hover={{ color: '#ff1f7a' }}>
          ♥ ♡ ♥  я люблю тебе  ♥ ♡ ♥
        </Text>
      </Box>
    )
  })
}
